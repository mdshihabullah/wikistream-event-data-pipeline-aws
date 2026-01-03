"""
WikiStream Pipeline Configuration
=================================
Central configuration for all pipeline components.
"""

import os
from typing import Set

# =============================================================================
# AWS Configuration
# =============================================================================

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
PROJECT_NAME = "wikistream"

# Resource naming
NAME_PREFIX = f"{PROJECT_NAME}-{ENVIRONMENT}"

# =============================================================================
# S3 Configuration
# =============================================================================

# Will be populated from Terraform output
S3_DATA_BUCKET = os.environ.get("S3_DATA_BUCKET", f"{NAME_PREFIX}-data")
S3_TABLES_BUCKET = os.environ.get("S3_TABLES_BUCKET", f"{NAME_PREFIX}-tables")

# Paths
CHECKPOINT_PATH = f"s3://{S3_DATA_BUCKET}/checkpoints"
SPARK_JOBS_PATH = f"s3://{S3_DATA_BUCKET}/spark/jobs"
LOGS_PATH = f"s3://{S3_DATA_BUCKET}/logs"

# =============================================================================
# Kafka Configuration
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")

KAFKA_TOPICS = {
    "raw_events": "raw-events",
    "dlq": "dlq-events",
}

# Consumer groups
CONSUMER_GROUPS = {
    "bronze_streaming": "bronze-streaming-cg",
    "dlq_processor": "dlq-processor-cg",
}

# =============================================================================
# Domain Filters
# =============================================================================

# High-activity Wikipedia editions
HIGH_ACTIVITY_DOMAINS: Set[str] = {
    "en.wikipedia.org",  # English - highest activity
    "de.wikipedia.org",  # German - 2nd most edits
    "ja.wikipedia.org",  # Japanese - 3rd most edits
    "fr.wikipedia.org",  # French - 4th most edits
    "zh.wikipedia.org",  # Chinese - major Asian market
    "es.wikipedia.org",  # Spanish - Latin America + Spain
    "ru.wikipedia.org",  # Russian - Eastern Europe
}

# Regional Wikipedia editions
REGIONAL_DOMAINS: dict[str, Set[str]] = {
    "asia_pacific": {
        "zh.wikipedia.org",
        "ja.wikipedia.org",
        "ko.wikipedia.org",
        "vi.wikipedia.org",
        "id.wikipedia.org",
        "th.wikipedia.org",
    },
    "europe": {
        "de.wikipedia.org",
        "fr.wikipedia.org",
        "it.wikipedia.org",
        "es.wikipedia.org",
        "pl.wikipedia.org",
        "nl.wikipedia.org",
    },
    "americas": {
        "en.wikipedia.org",
        "es.wikipedia.org",
        "pt.wikipedia.org",
    },
    "middle_east": {
        "ar.wikipedia.org",
        "fa.wikipedia.org",
        "he.wikipedia.org",
    },
}

# Combined target domains
TARGET_DOMAINS: Set[str] = HIGH_ACTIVITY_DOMAINS.union(
    *REGIONAL_DOMAINS.values()
)

# =============================================================================
# SLA Configuration
# =============================================================================

SLA_TARGETS = {
    "bronze_latency_minutes": 2,
    "silver_latency_minutes": 5,
    "gold_latency_minutes": 5,
    "dashboard_freshness_minutes": 5,
}

# =============================================================================
# Data Quality Thresholds
# =============================================================================

DATA_QUALITY = {
    "bronze": {
        "completeness_threshold": 0.95,  # 95% non-null required fields
        "uniqueness_check": True,
    },
    "silver": {
        "completeness_threshold": 0.98,
        "valid_domain_check": True,
        "deduplication_check": True,
    },
    "gold": {
        "aggregation_accuracy": 0.99,
        "timeliness_check": True,
    },
}

# =============================================================================
# Risk Scoring Configuration
# =============================================================================

RISK_THRESHOLDS = {
    "edits_per_hour_high": 50,
    "edits_per_hour_medium": 25,
    "large_deletions_high": 3,
    "large_deletions_medium": 1,
    "anonymous_ratio_threshold": 0.5,
    "cross_domain_suspicious": 5,
}

RISK_SCORE_WEIGHTS = {
    "velocity": 40,       # Max 40 points for edit velocity
    "deletions": 30,      # Max 30 points for large deletions
    "anonymous": 20,      # Max 20 points for anonymous activity
    "cross_domain": 10,   # Max 10 points for cross-domain activity
}

# =============================================================================
# Alerting Configuration
# =============================================================================

ALERTING = {
    "sns_topic_arn": os.environ.get("SNS_ALERTS_ARN", ""),
    "slack_webhook_url": os.environ.get("SLACK_WEBHOOK_URL", ""),
    "alert_on_risk_level": "HIGH",
    "alert_on_data_quality_failure": True,
}

# =============================================================================
# Processing Configuration
# =============================================================================

PROCESSING = {
    "bronze_trigger_interval": "30 seconds",
    "silver_schedule_minutes": 5,
    "gold_schedule_minutes": 5,
    "data_quality_schedule_minutes": 15,
    "dlq_reprocess_schedule_hours": 6,
    "lookback_hours": 1,
}

# =============================================================================
# Iceberg Table Properties
# =============================================================================

ICEBERG_PROPERTIES = {
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": "268435456",  # 256 MB
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10",
}

# =============================================================================
# EMR Serverless Configuration
# =============================================================================

EMR_CONFIG = {
    "release_label": "emr-7.5.0",
    "spark_submit_params": {
        "driver_cores": "2",
        "driver_memory": "4g",
        "executor_cores": "2",
        "executor_memory": "4g",
    },
}
