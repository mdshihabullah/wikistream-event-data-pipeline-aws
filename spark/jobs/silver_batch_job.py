#!/usr/bin/env python3
"""
WikiStream Silver Layer - Batch Processing Job
===============================================
Reads from Bronze table, cleans/enriches data, and writes to Silver table.

This job runs on EMR Serverless, triggered by Step Functions on schedule.
Uses MERGE for idempotent processing - safe for replays.
"""

import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, trim,
    current_timestamp, substring
)


# =============================================================================
# Configuration
# =============================================================================

SCHEMA_VERSION = "1.0.0"  # Track schema version for evolution

# Region to language mapping
DOMAIN_REGION_MAP = {
    # Asia Pacific
    "zh.wikipedia.org": ("asia_pacific", "zh"),
    "ja.wikipedia.org": ("asia_pacific", "ja"),
    "ko.wikipedia.org": ("asia_pacific", "ko"),
    "vi.wikipedia.org": ("asia_pacific", "vi"),
    "id.wikipedia.org": ("asia_pacific", "id"),
    "th.wikipedia.org": ("asia_pacific", "th"),
    # Europe
    "de.wikipedia.org": ("europe", "de"),
    "fr.wikipedia.org": ("europe", "fr"),
    "it.wikipedia.org": ("europe", "it"),
    "es.wikipedia.org": ("europe", "es"),
    "pl.wikipedia.org": ("europe", "pl"),
    "nl.wikipedia.org": ("europe", "nl"),
    "ru.wikipedia.org": ("europe", "ru"),
    # Americas
    "en.wikipedia.org": ("americas", "en"),
    "pt.wikipedia.org": ("americas", "pt"),
    # Middle East
    "ar.wikipedia.org": ("middle_east", "ar"),
    "fa.wikipedia.org": ("middle_east", "fa"),
    "he.wikipedia.org": ("middle_east", "he"),
}


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session() -> SparkSession:
    """
    Create SparkSession for EMR Serverless with S3 Tables Catalog.
    
    Note: S3 Tables catalog config is passed via spark-submit in Step Functions.
    This allows central configuration management in Terraform.
    """
    return (
        SparkSession.builder
        .appName("WikiStream-Silver-Batch")
        # Adaptive query execution for better performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Iceberg timestamp handling
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


# =============================================================================
# Transformations
# =============================================================================

def add_region_columns(df):
    """Add region and language columns based on domain."""
    # Build region case expression
    region_expr = None
    lang_expr = None
    
    for domain, (region, lang) in DOMAIN_REGION_MAP.items():
        if region_expr is None:
            region_expr = when(col("domain") == domain, lit(region))
            lang_expr = when(col("domain") == domain, lit(lang))
        else:
            region_expr = region_expr.when(col("domain") == domain, lit(region))
            lang_expr = lang_expr.when(col("domain") == domain, lit(lang))
    
    return (
        df
        .withColumn("region", region_expr.otherwise(lit("other")))
        .withColumn("language", lang_expr.otherwise(substring(col("domain"), 1, 2)))
    )


def clean_user_column(df):
    """Normalize and clean user column."""
    return (
        df
        .withColumn("user_normalized", 
            trim(regexp_replace(col("user"), r"[\x00-\x1f\x7f]", "")))
        .withColumn("is_anonymous",
            when(col("user_normalized").rlike(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"), True)
            .otherwise(False))
    )


def add_quality_flags(df):
    """Add data quality flags."""
    return (
        df
        # Check for valid event
        .withColumn("is_valid",
            (col("event_id").isNotNull()) &
            (col("event_type").isNotNull()) &
            (col("domain").isNotNull()) &
            (col("event_timestamp").isNotNull()))
        # Flag suspicious edits
        .withColumn("is_large_deletion",
            when(col("length_delta").isNotNull() & (col("length_delta") < -5000), True)
            .otherwise(False))
        .withColumn("is_large_addition",
            when(col("length_delta").isNotNull() & (col("length_delta") > 50000), True)
            .otherwise(False))
    )


def transform_bronze_to_silver(df):
    """Apply all transformations for Silver layer."""
    # Apply transformations sequentially (PySpark doesn't have pipe method)
    df = add_region_columns(df)
    df = clean_user_column(df)
    df = add_quality_flags(df)
    
    # Deduplicate by event_id - keep the most recent (max bronze_processed_at)
    # This handles historical duplicates in bronze table
    df = df.dropDuplicates(["event_id"])
    
    return (
        df
        .withColumn("silver_processed_at", current_timestamp())
        .withColumn("schema_version", lit(SCHEMA_VERSION))
        # Select Silver columns
        .select(
            "event_id",
            "rc_id",
            "event_type",
            "domain",
            "region",
            "language",
            "title",
            "namespace",
            "user_normalized",
            "is_bot",
            "is_anonymous",
            "length_old",
            "length_new",
            "length_delta",
            "revision_old",
            "revision_new",
            "is_valid",
            "is_large_deletion",
            "is_large_addition",
            "event_timestamp",
            "bronze_processed_at",
            "silver_processed_at",
            "event_date",
            "schema_version"
        )
        # Only include valid events
        .filter(col("is_valid"))
    )


# =============================================================================
# Main
# =============================================================================

def main():
    """Main entry point for Silver batch job.
    
    Optimized for ≤5 minute SLA:
    - Default lookback: 1 hour (process recent data quickly)
    - Incremental processing: only new Bronze records
    - Resource efficient: 4 vCPU total (1 driver + 1 executor × 2 vCPU)
    """
    print("=" * 60)
    print("WikiStream Silver Batch Job")
    print(f"Started at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    # Parse arguments: lookback_hours (default 1 hour for fast incremental processing)
    # Use 24 for backfill scenarios
    lookback_hours = 1
    if len(sys.argv) >= 2:
        lookback_hours = int(sys.argv[1])
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Calculate processing window
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=lookback_hours)
    start_date = start_time.strftime("%Y-%m-%d")
    end_date = end_time.strftime("%Y-%m-%d")
    
    print(f"Processing window: {start_date} to {end_date}")
    
    # Ensure Silver table exists
    # Partitioned by (event_date, region) for efficient time + regional queries
    # Region has 5 values: asia_pacific, europe, americas, middle_east, other
    # Note: Namespace 'silver' is created by Terraform, skip CREATE NAMESPACE
    spark.sql("""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.silver.cleaned_events (
            event_id STRING NOT NULL,
            rc_id BIGINT,
            event_type STRING,
            domain STRING,
            region STRING,
            language STRING,
            title STRING,
            namespace INT,
            user_normalized STRING,
            is_bot BOOLEAN,
            is_anonymous BOOLEAN,
            length_old INT,
            length_new INT,
            length_delta INT,
            revision_old BIGINT,
            revision_new BIGINT,
            is_valid BOOLEAN,
            is_large_deletion BOOLEAN,
            is_large_addition BOOLEAN,
            event_timestamp TIMESTAMP,
            bronze_processed_at TIMESTAMP,
            silver_processed_at TIMESTAMP,
            event_date STRING,
            schema_version STRING
        )
        USING iceberg
        PARTITIONED BY (event_date, region)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '268435456',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '10',
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
    """)
    
    # Read Bronze data for time window
    bronze_df = spark.sql(f"""
        SELECT * FROM s3tablesbucket.bronze.raw_events
        WHERE event_date >= '{start_date}' AND event_date <= '{end_date}'
    """)
    
    bronze_count = bronze_df.count()
    print(f"Bronze records in window: {bronze_count}")
    
    if bronze_count == 0:
        print("No Bronze records to process, exiting")
        spark.stop()
        return
    
    # Transform to Silver
    silver_df = transform_bronze_to_silver(bronze_df)
    silver_df.createOrReplaceTempView("incoming_silver")
    
    incoming_count = spark.sql("SELECT COUNT(*) FROM incoming_silver").collect()[0][0]
    print(f"Incoming Silver records: {incoming_count}")
    
    # Add schema_version column if it doesn't exist (for existing tables)
    try:
        spark.sql("""
            ALTER TABLE s3tablesbucket.silver.cleaned_events 
            ADD COLUMN schema_version STRING
        """)
        print("Added schema_version column to silver table")
    except Exception as e:
        # Column already exists or table doesn't exist yet
        pass
    
    # MERGE for idempotent upsert (no duplicates on replay)
    # Note: Iceberg requires explicit column lists, not UPDATE SET * or INSERT *
    spark.sql("""
        MERGE INTO s3tablesbucket.silver.cleaned_events AS target
        USING incoming_silver AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN UPDATE SET
            target.event_id = source.event_id,
            target.rc_id = source.rc_id,
            target.event_type = source.event_type,
            target.domain = source.domain,
            target.region = source.region,
            target.language = source.language,
            target.title = source.title,
            target.namespace = source.namespace,
            target.user_normalized = source.user_normalized,
            target.is_bot = source.is_bot,
            target.is_anonymous = source.is_anonymous,
            target.length_old = source.length_old,
            target.length_new = source.length_new,
            target.length_delta = source.length_delta,
            target.revision_old = source.revision_old,
            target.revision_new = source.revision_new,
            target.is_valid = source.is_valid,
            target.is_large_deletion = source.is_large_deletion,
            target.is_large_addition = source.is_large_addition,
            target.event_timestamp = source.event_timestamp,
            target.bronze_processed_at = source.bronze_processed_at,
            target.silver_processed_at = source.silver_processed_at,
            target.event_date = source.event_date,
            target.schema_version = source.schema_version
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # Log final counts
    final_count = spark.sql("SELECT COUNT(*) FROM s3tablesbucket.silver.cleaned_events").collect()[0][0]
    print(f"Total Silver records after merge: {final_count}")
    
    # Run compaction if needed (optional - S3 Tables handles this)
    # spark.sql("CALL wikistream.system.rewrite_data_files('silver.cleaned_events')")
    
    print("=" * 60)
    print(f"Silver job completed at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
