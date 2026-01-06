#!/usr/bin/env python3
"""
WikiStream Bronze Layer - Streaming Ingestion Job
==================================================
Reads raw events from MSK Kafka topic and writes to Bronze S3 Table (Iceberg).

This job runs on EMR Serverless with Spark Structured Streaming.
Features:
- Exactly-once semantics via checkpointing
- Watermarking for late/out-of-order events  
- Schema evolution support via mergeSchema
- Idempotent MERGE with deterministic event_id
- DLQ integration for malformed records
"""

import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, get_json_object, lit, to_timestamp, 
    current_timestamp, date_format, hour, when, md5, concat_ws,
    coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, LongType, TimestampType
)


# =============================================================================
# Configuration
# =============================================================================

# S3 paths (passed via Spark conf or args)
S3_BUCKET = "wikistream-dev-data-028210902207"  # Override via spark-submit
S3_TABLES_BUCKET = "wikistream-dev-tables"
# Note: CHECKPOINT_PATH is computed in main() after S3_BUCKET is updated from args

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ""  # Set via spark-submit
KAFKA_TOPIC = "raw-events"
KAFKA_DLQ_TOPIC = "dlq-events"

# Processing config
TRIGGER_INTERVAL = "3 minutes"  # For reducing no of snapshots 
WATERMARK_DELAY = "10 minutes"    # Allow late events up to 10 minutes
SCHEMA_VERSION = "1.0.0"         # Track schema version for evolution


# =============================================================================
# Schemas
# =============================================================================

# Schema for the enriched event from producer
KAFKA_VALUE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("data", StringType(), True),  # JSON string
    StructField("_processing", StructType([
        StructField("ingested_at", StringType(), True),
        StructField("producer_version", StringType(), True),
        StructField("environment", StringType(), True),
    ]), True)
])

# Flattened bronze schema for Iceberg table
BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("rc_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("domain", StringType(), True),
    StructField("title", StringType(), True),
    StructField("title_url", StringType(), True),
    StructField("user", StringType(), True),
    StructField("is_bot", BooleanType(), True),
    StructField("comment", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("server_name", StringType(), True),
    StructField("length_old", IntegerType(), True),
    StructField("length_new", IntegerType(), True),
    StructField("length_delta", IntegerType(), True),
    StructField("revision_old", LongType(), True),
    StructField("revision_new", LongType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("producer_ingested_at", TimestampType(), True),
    StructField("bronze_processed_at", TimestampType(), True),
    StructField("event_date", StringType(), True),
    StructField("event_hour", IntegerType(), True),
])


# =============================================================================
# Main Processing
# =============================================================================

def create_spark_session() -> SparkSession:
    """Create SparkSession configured for EMR Serverless and S3 Tables Catalog.
    
    Note: The S3 Tables catalog configuration (warehouse ARN) is passed via spark-submit
    parameters in production. Using getOrCreate() to preserve existing session configs.
    """
    return (
        SparkSession.builder
        .appName("WikiStream-Bronze-Streaming")
        # Iceberg extensions (only set if not already configured)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Iceberg write settings
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        # EMR Serverless optimizations
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Note: spark.sql.catalog.s3tablesbucket.* configs are passed via spark-submit
        # to avoid hardcoding account ID. Do NOT set defaults here.
        .getOrCreate()
    )


def transform_to_bronze(df):
    """
    Transform raw Kafka messages to Bronze table format.
    
    Features:
    - Watermarking for late/out-of-order event handling
    - Deterministic event_id generation for idempotent deduplication
    - Schema version tracking for evolution
    - Handles missing/malformed fields gracefully
    """
    return (
        df
        # Parse Kafka value JSON
        .select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), KAFKA_VALUE_SCHEMA).alias("parsed")
        )
        # Apply watermark for late event handling (5 minutes tolerance)
        .withWatermark("kafka_timestamp", WATERMARK_DELAY)
        # Extract event data
        .select(
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("parsed.id").alias("source_event_id"),
            col("parsed.data").alias("data_json"),
            col("parsed._processing.ingested_at").alias("producer_ingested_at_str"),
        )
        # Parse nested data JSON with null handling
        .select(
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("source_event_id"),
            col("producer_ingested_at_str"),
            coalesce(get_json_object(col("data_json"), "$.id").cast("long"), lit(-1)).alias("rc_id"),
            coalesce(get_json_object(col("data_json"), "$.type"), lit("unknown")).alias("event_type"),
            get_json_object(col("data_json"), "$.namespace").cast("int").alias("namespace"),
            get_json_object(col("data_json"), "$.meta.domain").alias("domain"),
            get_json_object(col("data_json"), "$.title").alias("title"),
            get_json_object(col("data_json"), "$.title_url").alias("title_url"),
            get_json_object(col("data_json"), "$.user").alias("user"),
            coalesce(get_json_object(col("data_json"), "$.bot").cast("boolean"), lit(False)).alias("is_bot"),
            get_json_object(col("data_json"), "$.comment").alias("comment"),
            get_json_object(col("data_json"), "$.wiki").alias("wiki"),
            get_json_object(col("data_json"), "$.server_name").alias("server_name"),
            get_json_object(col("data_json"), "$.length.old").cast("int").alias("length_old"),
            get_json_object(col("data_json"), "$.length.new").cast("int").alias("length_new"),
            get_json_object(col("data_json"), "$.revision.old").cast("long").alias("revision_old"),
            get_json_object(col("data_json"), "$.revision.new").cast("long").alias("revision_new"),
            get_json_object(col("data_json"), "$.timestamp").cast("long").alias("event_ts_unix"),
            get_json_object(col("data_json"), "$.meta.dt").alias("event_dt_str"),
        )
        # Generate deterministic event_id for idempotent deduplication
        # Combines source_event_id with domain+rc_id as fallback
        .withColumn(
            "event_id",
            when(col("source_event_id").isNotNull(), col("source_event_id"))
            .otherwise(
                md5(concat_ws("-", 
                    coalesce(col("domain"), lit("unknown")),
                    coalesce(col("rc_id").cast("string"), lit("0")),
                    coalesce(col("event_ts_unix").cast("string"), lit("0"))
                ))
            )
        )
        # Compute derived fields
        .withColumn(
            "length_delta",
            when(col("length_new").isNotNull() & col("length_old").isNotNull(),
                 col("length_new") - col("length_old"))
            .otherwise(None)
        )
        .withColumn(
            "event_timestamp",
            when(col("event_ts_unix").isNotNull(),
                 col("event_ts_unix").cast("timestamp"))
            .otherwise(to_timestamp(col("event_dt_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        )
        .withColumn(
            "producer_ingested_at",
            to_timestamp(col("producer_ingested_at_str"))
        )
        .withColumn("bronze_processed_at", current_timestamp())
        .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("schema_version", lit(SCHEMA_VERSION))
        # Select final columns
        .select(
            "event_id", "kafka_topic", "kafka_partition", "kafka_offset", 
            "kafka_timestamp", "rc_id", "event_type", "namespace", "domain",
            "title", "title_url", "user", "is_bot", "comment", "wiki",
            "server_name", "length_old", "length_new", "length_delta",
            "revision_old", "revision_new", "event_timestamp",
            "producer_ingested_at", "bronze_processed_at", "event_date", "event_hour",
            "schema_version"
        )
        # Filter out completely invalid records (no event_id means malformed)
        .filter(col("event_id").isNotNull())
        # Drop duplicates within the micro-batch (handles Kafka replays)
        .dropDuplicates(["event_id"])
    )


def write_to_iceberg(batch_df, batch_id):
    """
    Write micro-batch to Bronze Iceberg table.
    
    Features:
    - MERGE for idempotent upserts (handles duplicates from Kafka replays)
    - CloudWatch metrics publishing for monitoring
    - Processing latency tracking
    """
    import boto3
    from datetime import datetime
    
    batch_start = datetime.utcnow()
    record_count = batch_df.count()
    
    if record_count == 0:
        print(f"Batch {batch_id}: Empty batch, skipping")
        return
    
    batch_df.createOrReplaceTempView("incoming_events")
    
    spark = batch_df.sparkSession
    
    # MERGE INTO for idempotent upserts - prevents duplicates on replay/backfill
    spark.sql("""
        MERGE INTO s3tablesbucket.bronze.raw_events AS target
        USING incoming_events AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    batch_end = datetime.utcnow()
    latency_ms = (batch_end - batch_start).total_seconds() * 1000
    
    print(f"Batch {batch_id}: Merged {record_count} records in {latency_ms:.0f}ms")
    
    # Publish metrics to CloudWatch
    try:
        cloudwatch = boto3.client("cloudwatch", region_name="us-east-1")
        cloudwatch.put_metric_data(
            Namespace="WikiStream/Pipeline",
            MetricData=[
                {
                    "MetricName": "BronzeRecordsProcessed",
                    "Value": record_count,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "Layer", "Value": "bronze"}]
                },
                {
                    "MetricName": "ProcessingLatencyMs",
                    "Value": latency_ms,
                    "Unit": "Milliseconds",
                    "Dimensions": [{"Name": "Layer", "Value": "bronze"}]
                },
                {
                    "MetricName": "BronzeBatchCompleted",
                    "Value": 1,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "Layer", "Value": "bronze"}]
                }
            ]
        )
    except Exception as e:
        print(f"Warning: Failed to publish CloudWatch metrics: {e}")


def main():
    """Main entry point for streaming job."""
    print("=" * 60)
    print("WikiStream Bronze Streaming Job")
    print(f"Started at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    # Parse arguments
    global KAFKA_BOOTSTRAP_SERVERS, S3_BUCKET
    if len(sys.argv) >= 3:
        KAFKA_BOOTSTRAP_SERVERS = sys.argv[1]
        S3_BUCKET = sys.argv[2]
    
    # Compute checkpoint path after S3_BUCKET is updated from args
    CHECKPOINT_PATH = f"s3://{S3_BUCKET}/checkpoints/bronze"
    
    print(f"S3_BUCKET: {S3_BUCKET}")
    print(f"CHECKPOINT_PATH: {CHECKPOINT_PATH}")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Ensure Bronze table exists with schema evolution support
    # Note: Namespace created by Terraform, skip creation check
    # spark.sql("CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.bronze.raw_events (
            event_id STRING NOT NULL,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            rc_id BIGINT,
            event_type STRING,
            namespace INT,
            domain STRING,
            title STRING,
            title_url STRING,
            user STRING,
            is_bot BOOLEAN,
            comment STRING,
            wiki STRING,
            server_name STRING,
            length_old INT,
            length_new INT,
            length_delta INT,
            revision_old BIGINT,
            revision_new BIGINT,
            event_timestamp TIMESTAMP,
            producer_ingested_at TIMESTAMP,
            bronze_processed_at TIMESTAMP,
            event_date STRING,
            event_hour INT,
            schema_version STRING
        )
        USING iceberg
        PARTITIONED BY (event_date, event_hour)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '268435456',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '10',
            'format-version' = '3',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
    """)
    
    print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Checkpoint path: {CHECKPOINT_PATH}")
    
    # Read from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", "wikistream-bronze-streaming-consumer")
        .option("startingOffsets", "latest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .option("kafka.sasl.jaas.config", 
                "software.amazon.msk.auth.iam.IAMLoginModule required;")
        .option("kafka.sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
        .load()
    )
    
    # Transform and write
    checkpoint_path = f"s3://{S3_BUCKET}/checkpoints/bronze"
    print(f"Using checkpoint path: {checkpoint_path}")
    (
        transform_to_bronze(kafka_df)
        .writeStream
        .foreachBatch(write_to_iceberg)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
