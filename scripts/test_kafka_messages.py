#!/usr/bin/env python3
"""
Test Script: Send Manual Messages to MSK
=========================================
Tests the WikiStream pipeline by sending sample Wikipedia events to Kafka.
"""

import json
import sys
import time
from datetime import datetime

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer

# Configuration from command line
if len(sys.argv) < 2:
    print("Usage: python test_kafka_messages.py <MSK_BOOTSTRAP_SERVERS>")
    print("Example: python test_kafka_messages.py b-1.wikistream-dev-msk.xxxxx.kafka.us-east-1.amazonaws.com:9098")
    sys.exit(1)

BOOTSTRAP_SERVERS = sys.argv[1]
AWS_REGION = "us-east-1"
TOPIC = "raw-events"

# Sample Wikipedia events (realistic format)
SAMPLE_EVENTS = [
    {
        "id": "test-event-001",
        "data": json.dumps({
            "id": 1234567890,
            "type": "edit",
            "namespace": 0,
            "title": "Python_(programming_language)",
            "comment": "Updated syntax examples",
            "timestamp": 1704200400,
            "user": "TestUser123",
            "bot": False,
            "length": {
                "old": 45000,
                "new": 45150
            },
            "revision": {
                "old": 123456789,
                "new": 123456790
            },
            "meta": {
                "domain": "en.wikipedia.org",
                "dt": datetime.utcnow().isoformat() + "Z",
                "id": "test-event-001"
            },
            "server_name": "en.wikipedia.org",
            "wiki": "enwiki"
        }),
        "_processing": {
            "ingested_at": datetime.utcnow().isoformat() + "Z",
            "producer_version": "1.0.0-test",
            "environment": "test"
        }
    },
    {
        "id": "test-event-002",
        "data": json.dumps({
            "id": 1234567891,
            "type": "new",
            "namespace": 0,
            "title": "Apache_Iceberg_V3",
            "comment": "Created new article about Iceberg V3 features",
            "timestamp": 1704200401,
            "user": "DataEngineer456",
            "bot": False,
            "length": {
                "old": 0,
                "new": 5000
            },
            "revision": {
                "old": 0,
                "new": 987654321
            },
            "meta": {
                "domain": "en.wikipedia.org",
                "dt": datetime.utcnow().isoformat() + "Z",
                "id": "test-event-002"
            },
            "server_name": "en.wikipedia.org",
            "wiki": "enwiki"
        }),
        "_processing": {
            "ingested_at": datetime.utcnow().isoformat() + "Z",
            "producer_version": "1.0.0-test",
            "environment": "test"
        }
    },
    {
        "id": "test-event-003",
        "data": json.dumps({
            "id": 1234567892,
            "type": "edit",
            "namespace": 0,
            "title": "Tokio",
            "comment": "Added information about city landmarks",
            "timestamp": 1704200402,
            "user": "JapanEditor",
            "bot": False,
            "length": {
                "old": 78000,
                "new": 79500
            },
            "revision": {
                "old": 555666777,
                "new": 555666778
            },
            "meta": {
                "domain": "ja.wikipedia.org",
                "dt": datetime.utcnow().isoformat() + "Z",
                "id": "test-event-003"
            },
            "server_name": "ja.wikipedia.org",
            "wiki": "jawiki"
        }),
        "_processing": {
            "ingested_at": datetime.utcnow().isoformat() + "Z",
            "producer_version": "1.0.0-test",
            "environment": "test"
        }
    }
]


def _msk_iam_oauth_cb(oauth_config):
    """OAuth callback for MSK IAM authentication."""
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
    return auth_token, expiry_ms / 1000.0


def create_producer():
    """Create Kafka producer with MSK IAM auth."""
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'wikistream-test-producer',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': _msk_iam_oauth_cb,
        'acks': 'all',
    }
    return Producer(config)


def delivery_report(err, msg):
    """Callback for delivery reports."""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✓ Message delivered to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}')


def main():
    print("=" * 60)
    print("WikiStream Test - Manual Kafka Messages")
    print("=" * 60)
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
    print(f"Topic: {TOPIC}")
    print(f"Sample events: {len(SAMPLE_EVENTS)}")
    print("=" * 60)
    print()
    
    # Create producer
    print("Creating Kafka producer...")
    producer = create_producer()
    print("✓ Producer created")
    print()
    
    # Send test messages
    print("Sending test messages...")
    for i, event in enumerate(SAMPLE_EVENTS, 1):
        try:
            # Serialize to JSON
            value = json.dumps(event).encode('utf-8')
            key = event['id'].encode('utf-8')
            
            # Send message
            producer.produce(
                TOPIC,
                value=value,
                key=key,
                callback=delivery_report
            )
            
            print(f"  [{i}/{len(SAMPLE_EVENTS)}] Sent: {event['id']}")
            
            # Poll to handle delivery callbacks
            producer.poll(0)
            
            # Small delay between messages
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error sending message {i}: {e}")
    
    # Wait for all messages to be delivered
    print()
    print("Flushing producer...")
    producer.flush(timeout=10)
    
    print()
    print("=" * 60)
    print("✓ Test complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Check Bronze streaming job logs for processing")
    print("2. Wait ~5 minutes for Silver/Gold batch jobs")
    print("3. Query tables:")
    print("   SELECT COUNT(*) FROM s3tablesbucket.bronze.raw_events")
    print("   SELECT COUNT(*) FROM s3tablesbucket.silver.cleaned_events")


if __name__ == "__main__":
    main()

