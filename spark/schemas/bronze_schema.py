"""
Bronze Layer Schema Definition
==============================
Schema for raw Wikimedia recentchange events ingested from Kafka.
Based on actual Wikimedia EventStreams schema.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    BooleanType,
    TimestampType,
    ArrayType,
    MapType,
)


# =============================================================================
# KAFKA MESSAGE SCHEMA (outer wrapper)
# =============================================================================

KAFKA_ID_SCHEMA = StructType([
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("timestamp", LongType(), True),
])


# =============================================================================
# WIKIMEDIA RECENTCHANGE EVENT SCHEMA
# =============================================================================

META_SCHEMA = StructType([
    StructField("uri", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("id", StringType(), False),  # Primary unique identifier
    StructField("domain", StringType(), False),
    StructField("stream", StringType(), True),
    StructField("dt", StringType(), False),  # ISO8601 timestamp
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
])

LENGTH_SCHEMA = StructType([
    StructField("old", IntegerType(), True),
    StructField("new", IntegerType(), True),
])

REVISION_SCHEMA = StructType([
    StructField("old", LongType(), True),
    StructField("new", LongType(), True),
])

# The main data payload from Wikimedia
RECENTCHANGE_DATA_SCHEMA = StructType([
    # Schema identifier
    StructField("$schema", StringType(), True),
    
    # Metadata block
    StructField("meta", META_SCHEMA, False),
    
    # Event identifiers
    StructField("id", LongType(), False),  # Wikimedia internal RC ID
    
    # Event classification
    StructField("type", StringType(), False),  # edit, new, log, categorize
    StructField("namespace", IntegerType(), True),
    
    # Content identifiers
    StructField("title", StringType(), True),
    StructField("title_url", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("parsedcomment", StringType(), True),
    
    # Timestamps
    StructField("timestamp", LongType(), False),  # Unix epoch
    
    # User information
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    
    # Change details
    StructField("minor", BooleanType(), True),
    StructField("patrolled", BooleanType(), True),
    StructField("length", LENGTH_SCHEMA, True),
    StructField("revision", REVISION_SCHEMA, True),
    
    # Server information
    StructField("server_url", StringType(), True),
    StructField("server_name", StringType(), True),
    StructField("server_script_path", StringType(), True),
    StructField("wiki", StringType(), True),
    
    # Notification
    StructField("notify_url", StringType(), True),
    
    # Log-specific fields (for type="log")
    StructField("log_id", LongType(), True),
    StructField("log_type", StringType(), True),
    StructField("log_action", StringType(), True),
    StructField("log_params", MapType(StringType(), StringType()), True),
    StructField("log_action_comment", StringType(), True),
])


# =============================================================================
# BRONZE TABLE SCHEMA (flattened for Iceberg storage)
# =============================================================================

BRONZE_TABLE_SCHEMA = StructType([
    # ===== Primary Key & Deduplication =====
    StructField("event_id", StringType(), False),  # meta.id - unique identifier
    StructField("rc_id", LongType(), False),       # Wikimedia RC ID
    
    # ===== Kafka Metadata =====
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", LongType(), True),
    
    # ===== Event Classification =====
    StructField("event_type", StringType(), False),  # edit, new, log, categorize
    StructField("namespace", IntegerType(), True),
    StructField("wiki", StringType(), True),
    
    # ===== Domain & Server =====
    StructField("domain", StringType(), False),
    StructField("server_url", StringType(), True),
    StructField("server_name", StringType(), True),
    
    # ===== Content =====
    StructField("title", StringType(), True),
    StructField("title_url", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("parsed_comment", StringType(), True),
    
    # ===== User Information =====
    StructField("user", StringType(), True),
    StructField("is_bot", BooleanType(), True),
    
    # ===== Change Flags =====
    StructField("is_minor", BooleanType(), True),
    StructField("is_patrolled", BooleanType(), True),
    
    # ===== Size Changes =====
    StructField("length_old", IntegerType(), True),
    StructField("length_new", IntegerType(), True),
    StructField("length_delta", IntegerType(), True),  # Computed: new - old
    
    # ===== Revision Information =====
    StructField("revision_old", LongType(), True),
    StructField("revision_new", LongType(), True),
    
    # ===== Log-specific (nullable) =====
    StructField("log_id", LongType(), True),
    StructField("log_type", StringType(), True),
    StructField("log_action", StringType(), True),
    
    # ===== Timestamps =====
    StructField("event_timestamp", TimestampType(), False),  # Parsed from dt
    StructField("unix_timestamp", LongType(), False),        # Original unix ts
    
    # ===== Metadata URIs =====
    StructField("meta_uri", StringType(), True),
    StructField("meta_request_id", StringType(), True),
    StructField("notify_url", StringType(), True),
    
    # ===== Schema Version =====
    StructField("schema_version", StringType(), True),
    
    # ===== Ingestion Metadata =====
    StructField("ingested_at", TimestampType(), False),
    
    # ===== Partitioning Columns =====
    StructField("event_date", StringType(), False),  # YYYY-MM-DD
    StructField("event_hour", IntegerType(), False), # 0-23
])


# =============================================================================
# DLQ SCHEMA
# =============================================================================

DLQ_SCHEMA = StructType([
    StructField("original_message", StringType(), False),
    StructField("error_message", StringType(), False),
    StructField("error_type", StringType(), False),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", LongType(), True),
    StructField("failed_at", TimestampType(), False),
    StructField("retry_count", IntegerType(), False),
])


# =============================================================================
# SCHEMA DDL FOR ICEBERG TABLE CREATION
# =============================================================================

BRONZE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table} (
    event_id STRING NOT NULL,
    rc_id BIGINT NOT NULL,
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp BIGINT,
    event_type STRING NOT NULL,
    namespace INT,
    wiki STRING,
    domain STRING NOT NULL,
    server_url STRING,
    server_name STRING,
    title STRING,
    title_url STRING,
    comment STRING,
    parsed_comment STRING,
    user STRING,
    is_bot BOOLEAN,
    is_minor BOOLEAN,
    is_patrolled BOOLEAN,
    length_old INT,
    length_new INT,
    length_delta INT,
    revision_old BIGINT,
    revision_new BIGINT,
    log_id BIGINT,
    log_type STRING,
    log_action STRING,
    event_timestamp TIMESTAMP NOT NULL,
    unix_timestamp BIGINT NOT NULL,
    meta_uri STRING,
    meta_request_id STRING,
    notify_url STRING,
    schema_version STRING,
    ingested_at TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (event_date STRING, event_hour INT)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100',
    'write.spark.fanout.enabled' = 'true',
    'commit.retry.num-retries' = '4',
    'commit.retry.min-wait-ms' = '100'
)
"""


DLQ_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.dlq_events (
    original_message STRING NOT NULL,
    error_message STRING NOT NULL,
    error_type STRING NOT NULL,
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp BIGINT,
    failed_at TIMESTAMP NOT NULL,
    retry_count INT NOT NULL,
    failed_date STRING NOT NULL
)
USING iceberg
PARTITIONED BY (failed_date STRING)
"""



