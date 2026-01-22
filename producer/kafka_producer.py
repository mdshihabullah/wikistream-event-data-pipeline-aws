#!/usr/bin/env python3
"""
WikiStream Kafka Producer
=========================
Consumes Wikimedia EventStreams (SSE) and produces to Amazon MSK (KRaft).
Uses IAM authentication for MSK connectivity.

Runs on ECS Fargate for continuous, long-running stream processing.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Optional

import boto3
import sseclient
import requests
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer

# =============================================================================
# Configuration
# =============================================================================

# Environment variables (set in ECS task definition)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

# Topics
RAW_EVENTS_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "raw-events")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "dlq-events")

# Wikimedia SSE endpoint
WIKIMEDIA_SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# User-Agent header required by Wikimedia API etiquette
# See: https://meta.wikimedia.org/wiki/User-Agent_policy
USER_AGENT = "WikiStreamProducer/1.0 (https://github.com/mdshihabullah/wikistream-event-data-pipeline) Python/requests"

# Domain filter - high activity + regional wikis
TARGET_DOMAINS = {
    # High activity
    "en.wikipedia.org",
    "de.wikipedia.org",
    "ja.wikipedia.org",
    "fr.wikipedia.org",
    "zh.wikipedia.org",
    "es.wikipedia.org",
    "ru.wikipedia.org",
    # Asia-Pacific
    "ko.wikipedia.org",
    "vi.wikipedia.org",
    "id.wikipedia.org",
    "th.wikipedia.org",
    # Europe
    "it.wikipedia.org",
    "pl.wikipedia.org",
    "nl.wikipedia.org",
    # Americas
    "pt.wikipedia.org",
    # Middle East
    "ar.wikipedia.org",
    "fa.wikipedia.org",
    "he.wikipedia.org",
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("WikiStreamProducer")

# =============================================================================
# MSK IAM Authentication
# =============================================================================

def _msk_iam_oauth_cb(oauth_config):
    """
    OAuth callback for MSK IAM authentication.
    Called by librdkafka to generate auth tokens.
    
    Args:
        oauth_config: OAuth configuration string (unused, but required by callback signature)
        
    Returns:
        Tuple of (token, lifetime_ms) for authentication
    """
    try:
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        logger.debug(f"Generated MSK auth token (expires in {expiry_ms}ms)")
        return auth_token, expiry_ms / 1000.0  # Convert to seconds
    except Exception as e:
        logger.error(f"Failed to generate MSK auth token: {e}")
        raise


def create_kafka_producer(bootstrap_servers: str) -> Producer:
    """
    Create Kafka producer with MSK IAM authentication.
    
    Args:
        bootstrap_servers: MSK bootstrap servers (IAM endpoint)
        
    Returns:
        Configured Producer instance
    """
    config = {
        # Basic config
        'bootstrap.servers': bootstrap_servers,
        'client.id': f'wikistream-producer-{ENVIRONMENT}',
        
        # Security config for MSK IAM
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': _msk_iam_oauth_cb,
        
        # Producer config
        'acks': 'all',  # Wait for all replicas
        'retries': 3,
        'retry.backoff.ms': 1000,
        'compression.type': 'snappy',
        'linger.ms': 50,  # Small batching for latency
        'batch.size': 32768,  # 32KB batches
        'max.in.flight.requests.per.connection': 5,
        
        # Error handling
        'enable.idempotence': True,
        'max.in.flight': 5,
    }
    
    return Producer(config)


# =============================================================================
# Event Processing
# =============================================================================

def validate_event(event_data: dict) -> tuple[bool, Optional[str]]:
    """
    Validate incoming Wikimedia event.
    
    Args:
        event_data: Raw event dictionary
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ["meta", "type"]
    
    for field in required_fields:
        if field not in event_data:
            return False, f"Missing required field: {field}"
    
    if "meta" in event_data:
        meta = event_data["meta"]
        if not isinstance(meta, dict):
            return False, "meta field is not a dictionary"
        if "id" not in meta:
            return False, "Missing meta.id"
        if "domain" not in meta:
            return False, "Missing meta.domain"
        if "dt" not in meta:
            return False, "Missing meta.dt"
    
    return True, None


def should_process_event(event_data: dict) -> bool:
    """
    Check if event should be processed based on domain filter.
    
    Args:
        event_data: Event dictionary with meta.domain
        
    Returns:
        True if event should be processed
    """
    try:
        domain = event_data.get("meta", {}).get("domain", "")
        return domain in TARGET_DOMAINS
    except Exception:
        return False


def enrich_event(event_data: dict) -> dict:
    """
    Add processing metadata to event.
    
    Args:
        event_data: Original event
        
    Returns:
        Enriched event with processing metadata
        Note: 'data' is serialized as JSON string for Spark parsing
    """
    enriched = {
        "id": event_data.get("meta", {}).get("id"),
        "data": json.dumps(event_data),  # Serialize as JSON string for Spark
        "_processing": {
            "ingested_at": datetime.utcnow().isoformat() + "Z",
            "producer_version": "1.0.0",
            "environment": ENVIRONMENT,
        }
    }
    return enriched


# =============================================================================
# Main Processing Loop
# =============================================================================

class WikiStreamProducer:
    """Main producer class for WikiStream pipeline."""
    
    def __init__(self):
        self.producer: Optional[Producer] = None
        self.running = True
        self.stats = {
            "events_received": 0,
            "events_processed": 0,
            "events_filtered": 0,
            "events_dlq": 0,
            "errors": 0,
        }
        # Track last published values for delta calculations
        self.last_published_stats = {
            "events_received": 0,
            "events_processed": 0,
            "events_filtered": 0,
            "events_dlq": 0,
            "errors": 0,
        }
        self.last_stats_log = time.time()
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def _log_stats(self):
        """Log processing statistics periodically and publish to CloudWatch."""
        now = time.time()
        if now - self.last_stats_log >= 60:  # Every minute
            logger.info(
                f"Stats: received={self.stats['events_received']}, "
                f"processed={self.stats['events_processed']}, "
                f"filtered={self.stats['events_filtered']}, "
                f"dlq={self.stats['events_dlq']}, "
                f"errors={self.stats['errors']}"
            )
            
            # Publish metrics to CloudWatch
            self._publish_cloudwatch_metrics()
            
            self.last_stats_log = now
    
    def _publish_cloudwatch_metrics(self):
        """Publish producer metrics to CloudWatch for monitoring.
        
        Publishes incremental (delta) values since last publish to enable
        per-minute rate calculations with CloudWatch Sum statistic.
        """
        try:
            cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)
            
            # Calculate deltas since last publish (for per-minute rates)
            metrics = [
                {
                    'MetricName': 'EventsReceived',
                    'Value': self.stats['events_received'] - self.last_published_stats['events_received'],
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'EventsProcessed',
                    'Value': self.stats['events_processed'] - self.last_published_stats['events_processed'],
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'EventsFiltered',
                    'Value': self.stats['events_filtered'] - self.last_published_stats['events_filtered'],
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'DLQMessagesProduced',
                    'Value': self.stats['events_dlq'] - self.last_published_stats['events_dlq'],
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'ValidationErrors',
                    'Value': self.stats['errors'] - self.last_published_stats['errors'],
                    'Unit': 'Count'
                }
            ]
            
            cloudwatch.put_metric_data(
                Namespace='WikiStream/Producer',
                MetricData=metrics
            )
            
            # Update last published values
            self.last_published_stats = self.stats.copy()
            
            logger.debug("Published incremental metrics to CloudWatch")
        except Exception as e:
            logger.warning(f"Failed to publish CloudWatch metrics: {e}")
    
    def _delivery_callback(self, err, msg):
        """Callback for producer delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
            self.stats["errors"] += 1
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def _connect_producer(self) -> bool:
        """Connect to MSK with retry logic."""
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to MSK (attempt {attempt + 1}/{max_retries})...")
                self.producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
                logger.info("Successfully created MSK producer")
                return True
            except Exception as e:
                logger.error(f"Connection error: {e}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60s
        
        logger.error("Failed to connect to MSK after all retries")
        return False
    
    def _send_to_topic(self, topic: str, key: Optional[str], value: dict):
        """Send message to Kafka topic with error handling."""
        try:
            # Serialize value to JSON
            value_bytes = json.dumps(value).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None
            
            # Produce message (async)
            self.producer.produce(
                topic,
                value=value_bytes,
                key=key_bytes,
                callback=self._delivery_callback
            )
            
            # Poll to handle callbacks (non-blocking)
            self.producer.poll(0)
            
        except BufferError as e:
            logger.warning(f"Local producer queue full, waiting: {e}")
            self.producer.poll(1)  # Wait for queue to clear
            self.stats["errors"] += 1
        except Exception as e:
            logger.error(f"Kafka error: {e}")
            self.stats["errors"] += 1
    
    def _process_event(self, event_data: dict):
        """Process a single event."""
        self.stats["events_received"] += 1
        
        # Validate
        is_valid, error_msg = validate_event(event_data)
        if not is_valid:
            logger.debug(f"Invalid event: {error_msg}")
            dlq_record = {
                "original_event": event_data,
                "error": error_msg,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            self._send_to_topic(DLQ_TOPIC, None, dlq_record)
            self.stats["events_dlq"] += 1
            return
        
        # Filter by domain
        if not should_process_event(event_data):
            self.stats["events_filtered"] += 1
            return
        
        # Enrich and send
        enriched = enrich_event(event_data)
        event_id = enriched["id"]
        self._send_to_topic(RAW_EVENTS_TOPIC, event_id, enriched)
        self.stats["events_processed"] += 1
    
    def run(self):
        """Main processing loop."""
        logger.info("Starting WikiStream Producer")
        logger.info(f"Target domains: {len(TARGET_DOMAINS)}")
        logger.info(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS[:50]}...")
        
        # Connect to MSK
        if not self._connect_producer():
            sys.exit(1)
        
        # Connect to SSE stream
        while self.running:
            try:
                logger.info("Connecting to Wikimedia EventStreams...")
                response = requests.get(
                    WIKIMEDIA_SSE_URL,
                    stream=True,
                    timeout=(10, 300),  # Connect timeout, read timeout
                    headers={
                        "Accept": "text/event-stream",
                        "User-Agent": USER_AGENT,
                    }
                )
                response.raise_for_status()
                
                client = sseclient.SSEClient(response)
                logger.info("Connected to Wikimedia EventStreams")
                
                for event in client.events():
                    if not self.running:
                        break
                    
                    if event.data:
                        try:
                            event_data = json.loads(event.data)
                            self._process_event(event_data)
                        except json.JSONDecodeError as e:
                            logger.warning(f"JSON decode error: {e}")
                            self.stats["errors"] += 1
                    
                    self._log_stats()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"SSE connection error: {e}")
                if self.running:
                    time.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                if self.running:
                    time.sleep(5)
        
        # Cleanup
        logger.info("Flushing producer...")
        if self.producer:
            self.producer.flush(timeout=10)
        
        logger.info(f"Final stats: {self.stats}")
        logger.info("Producer shutdown complete")


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    # Validate configuration
    if not KAFKA_BOOTSTRAP_SERVERS:
        logger.error("KAFKA_BOOTSTRAP_SERVERS environment variable not set")
        sys.exit(1)
    
    producer = WikiStreamProducer()
    producer.run()
