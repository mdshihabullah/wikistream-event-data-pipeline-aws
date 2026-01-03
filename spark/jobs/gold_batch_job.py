#!/usr/bin/env python3
"""
WikiStream Gold Layer - Aggregation & Analytics Job
====================================================
Reads from Silver table, computes aggregations and risk scores for Gold layer.

This job runs on EMR Serverless, triggered by Step Functions on schedule.
Produces tables optimized for QuickSight dashboards:
  - gold.hourly_stats: Hourly metrics by domain
  - gold.risk_scores: Entity-level risk scores with evidence
"""

import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, max as spark_max,
    when, lit, current_timestamp, date_format, hour, concat_ws,
    collect_list, to_json, struct, round as spark_round
)
from pyspark.sql.window import Window


# =============================================================================
# Configuration
# =============================================================================

# Risk scoring thresholds
RISK_THRESHOLDS = {
    "edits_per_hour": 50,      # High if user makes >50 edits/hour
    "large_deletions": 3,      # High if >3 large deletions
    "bot_edit_ratio": 0.8,     # Flag if domain >80% bot edits
    "anonymous_ratio": 0.5,    # Flag if user >50% anonymous edits
}


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session() -> SparkSession:
    """
    Create SparkSession for EMR Serverless with S3 Tables Catalog.
    
    Note: S3 Tables catalog config is passed via spark-submit in Step Functions.
    """
    return (
        SparkSession.builder
        .appName("WikiStream-Gold-Batch")
        # Adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Iceberg timestamp handling
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


# =============================================================================
# Aggregations
# =============================================================================

def compute_hourly_stats(spark, start_date: str, end_date: str):
    """
    Compute hourly statistics by domain.
    
    Metrics:
    - Total events, unique users, unique pages
    - Bytes added/removed
    - Bot vs human percentages
    - Edit type breakdown
    """
    print("Computing hourly stats...")
    
    hourly_df = spark.sql(f"""
        SELECT
            event_date AS stat_date,
            HOUR(event_timestamp) AS stat_hour,
            domain,
            region,
            
            -- Volume metrics
            COUNT(*) AS total_events,
            COUNT(DISTINCT user_normalized) AS unique_users,
            COUNT(DISTINCT title) AS unique_pages,
            
            -- Content metrics
            SUM(CASE WHEN length_delta > 0 THEN length_delta ELSE 0 END) AS bytes_added,
            SUM(CASE WHEN length_delta < 0 THEN ABS(length_delta) ELSE 0 END) AS bytes_removed,
            AVG(ABS(COALESCE(length_delta, 0))) AS avg_edit_size,
            
            -- User metrics
            SUM(CASE WHEN is_bot = true THEN 1 ELSE 0 END) AS bot_edits,
            SUM(CASE WHEN is_bot = false THEN 1 ELSE 0 END) AS human_edits,
            ROUND(100.0 * SUM(CASE WHEN is_bot = true THEN 1 ELSE 0 END) / COUNT(*), 2) AS bot_percentage,
            SUM(CASE WHEN is_anonymous = true THEN 1 ELSE 0 END) AS anonymous_edits,
            
            -- Edit type breakdown
            SUM(CASE WHEN event_type = 'edit' THEN 1 ELSE 0 END) AS type_edit,
            SUM(CASE WHEN event_type = 'new' THEN 1 ELSE 0 END) AS type_new,
            SUM(CASE WHEN event_type = 'categorize' THEN 1 ELSE 0 END) AS type_categorize,
            SUM(CASE WHEN event_type = 'log' THEN 1 ELSE 0 END) AS type_log,
            
            -- Quality flags
            SUM(CASE WHEN is_large_deletion = true THEN 1 ELSE 0 END) AS large_deletions,
            SUM(CASE WHEN is_large_addition = true THEN 1 ELSE 0 END) AS large_additions,
            
            CURRENT_TIMESTAMP() AS gold_processed_at
            
        FROM s3tablesbucket.silver.cleaned_events
        WHERE event_date >= '{start_date}' AND event_date <= '{end_date}'
        GROUP BY event_date, HOUR(event_timestamp), domain, region
    """)
    
    return hourly_df


def compute_risk_scores(spark, start_date: str, end_date: str):
    """
    Compute risk scores for users based on edit patterns.
    
    Risk factors:
    - High edit velocity
    - Multiple large deletions
    - Mostly anonymous edits
    - Suspicious patterns
    """
    print("Computing risk scores...")
    
    # User-level metrics
    user_metrics = spark.sql(f"""
        SELECT
            event_date AS stat_date,
            user_normalized AS entity_id,
            'user' AS entity_type,
            
            COUNT(*) AS total_edits,
            COUNT(*) / 24.0 AS edits_per_hour_avg,
            MAX(HOUR(event_timestamp)) - MIN(HOUR(event_timestamp)) + 1 AS active_hours,
            COUNT(DISTINCT domain) AS domains_edited,
            COUNT(DISTINCT title) AS pages_edited,
            
            SUM(CASE WHEN is_large_deletion = true THEN 1 ELSE 0 END) AS large_deletions,
            SUM(CASE WHEN is_large_addition = true THEN 1 ELSE 0 END) AS large_additions,
            
            SUM(CASE WHEN is_anonymous = true THEN 1 ELSE 0 END) AS anonymous_edits,
            SUM(CASE WHEN is_bot = true THEN 1 ELSE 0 END) AS bot_edits,
            
            AVG(COALESCE(length_delta, 0)) AS avg_length_delta,
            MIN(length_delta) AS min_length_delta
            
        FROM s3tablesbucket.silver.cleaned_events
        WHERE event_date >= '{start_date}' AND event_date <= '{end_date}'
          AND is_bot = false  -- Exclude bots from risk scoring
          AND user_normalized IS NOT NULL
        GROUP BY event_date, user_normalized
        HAVING COUNT(*) >= 5  -- Only users with significant activity
    """)
    
    user_metrics.createOrReplaceTempView("user_metrics")
    
    # Compute risk score
    risk_df = spark.sql(f"""
        SELECT
            stat_date,
            entity_id,
            entity_type,
            total_edits,
            edits_per_hour_avg,
            large_deletions,
            domains_edited,
            
            -- Risk score calculation (0-100)
            LEAST(100, GREATEST(0,
                -- High velocity factor (0-40 points)
                CASE 
                    WHEN edits_per_hour_avg > {RISK_THRESHOLDS['edits_per_hour']} THEN 40
                    WHEN edits_per_hour_avg > {RISK_THRESHOLDS['edits_per_hour'] / 2} THEN 25
                    WHEN edits_per_hour_avg > 10 THEN 10
                    ELSE 0
                END
                -- Large deletions factor (0-30 points)
                + CASE
                    WHEN large_deletions > {RISK_THRESHOLDS['large_deletions']} THEN 30
                    WHEN large_deletions > 1 THEN 15
                    ELSE 0
                END
                -- Anonymous activity factor (0-20 points)
                + CASE
                    WHEN anonymous_edits > total_edits * {RISK_THRESHOLDS['anonymous_ratio']} THEN 20
                    ELSE 0
                END
                -- Cross-domain activity factor (0-10 points)
                + CASE
                    WHEN domains_edited > 5 AND edits_per_hour_avg > 20 THEN 10
                    ELSE 0
                END
            )) AS risk_score,
            
            -- Risk level
            CASE
                WHEN LEAST(100, GREATEST(0,
                    CASE WHEN edits_per_hour_avg > {RISK_THRESHOLDS['edits_per_hour']} THEN 40
                         WHEN edits_per_hour_avg > {RISK_THRESHOLDS['edits_per_hour'] / 2} THEN 25
                         ELSE 0 END
                    + CASE WHEN large_deletions > {RISK_THRESHOLDS['large_deletions']} THEN 30
                           WHEN large_deletions > 1 THEN 15 ELSE 0 END
                    + CASE WHEN anonymous_edits > total_edits * {RISK_THRESHOLDS['anonymous_ratio']} THEN 20 ELSE 0 END
                    + CASE WHEN domains_edited > 5 AND edits_per_hour_avg > 20 THEN 10 ELSE 0 END
                )) >= 70 THEN 'HIGH'
                WHEN LEAST(100, GREATEST(0,
                    CASE WHEN edits_per_hour_avg > {RISK_THRESHOLDS['edits_per_hour']} THEN 40
                         WHEN edits_per_hour_avg > {RISK_THRESHOLDS['edits_per_hour'] / 2} THEN 25
                         ELSE 0 END
                    + CASE WHEN large_deletions > {RISK_THRESHOLDS['large_deletions']} THEN 30
                           WHEN large_deletions > 1 THEN 15 ELSE 0 END
                    + CASE WHEN anonymous_edits > total_edits * {RISK_THRESHOLDS['anonymous_ratio']} THEN 20 ELSE 0 END
                )) >= 40 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS risk_level,
            
            -- Evidence JSON for alerting
            CONCAT('{{',
                '"edits_per_hour":', ROUND(edits_per_hour_avg, 2), ',',
                '"large_deletions":', large_deletions, ',',
                '"domains_edited":', domains_edited, ',',
                '"total_edits":', total_edits, ',',
                '"anonymous_edits":', anonymous_edits,
            '}}') AS evidence,
            
            -- Alert flag
            CASE WHEN LEAST(100, GREATEST(0,
                CASE WHEN edits_per_hour_avg > {RISK_THRESHOLDS['edits_per_hour']} THEN 40 ELSE 0 END
                + CASE WHEN large_deletions > {RISK_THRESHOLDS['large_deletions']} THEN 30 ELSE 0 END
            )) >= 70 THEN true ELSE false END AS alert_triggered,
            
            CURRENT_TIMESTAMP() AS gold_processed_at
            
        FROM user_metrics
    """)
    
    return risk_df


# =============================================================================
# Main
# =============================================================================

def main():
    """Main entry point for Gold batch job."""
    print("=" * 60)
    print("WikiStream Gold Batch Job")
    print(f"Started at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    # Parse arguments
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
    
    # Create Gold tables
    # Note: Namespace 'gold' is created by Terraform, skip CREATE NAMESPACE
    
    # Hourly stats table
    # Partitioned by (stat_date, region) for efficient time + regional queries
    # Region has 5 values: asia_pacific, europe, americas, middle_east, other
    spark.sql("""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.gold.hourly_stats (
            stat_date STRING NOT NULL,
            stat_hour INT NOT NULL,
            domain STRING NOT NULL,
            region STRING,
            total_events BIGINT,
            unique_users BIGINT,
            unique_pages BIGINT,
            bytes_added BIGINT,
            bytes_removed BIGINT,
            avg_edit_size DOUBLE,
            bot_edits BIGINT,
            human_edits BIGINT,
            bot_percentage DOUBLE,
            anonymous_edits BIGINT,
            type_edit BIGINT,
            type_new BIGINT,
            type_categorize BIGINT,
            type_log BIGINT,
            large_deletions BIGINT,
            large_additions BIGINT,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (stat_date, region)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '268435456',
            'format-version' = '2',
            'write.merge.mode' = 'copy-on-write',
            'write.delete.mode' = 'copy-on-write',
            'write.update.mode' = 'copy-on-write'
        )
    """)
    
    # Risk scores table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.gold.risk_scores (
            stat_date STRING NOT NULL,
            entity_id STRING NOT NULL,
            entity_type STRING,
            total_edits BIGINT,
            edits_per_hour_avg DOUBLE,
            large_deletions BIGINT,
            domains_edited BIGINT,
            risk_score DOUBLE,
            risk_level STRING,
            evidence STRING,
            alert_triggered BOOLEAN,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (stat_date)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '268435456',
            'format-version' = '2',
            'write.merge.mode' = 'copy-on-write',
            'write.delete.mode' = 'copy-on-write',
            'write.update.mode' = 'copy-on-write'
        )
    """)
    
    # Compute and write hourly stats
    hourly_df = compute_hourly_stats(spark, start_date, end_date)
    hourly_df.createOrReplaceTempView("incoming_hourly")
    
    hourly_count = spark.sql("SELECT COUNT(*) FROM incoming_hourly").collect()[0][0]
    print(f"Incoming hourly stats: {hourly_count}")
    
    if hourly_count > 0:
        # MERGE with partition columns (stat_date, region) + domain for unique key
        spark.sql("""
            MERGE INTO s3tablesbucket.gold.hourly_stats AS target
            USING incoming_hourly AS source
            ON target.stat_date = source.stat_date 
               AND target.region = source.region
               AND target.stat_hour = source.stat_hour 
               AND target.domain = source.domain
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged hourly stats")
    
    # Compute and write risk scores
    risk_df = compute_risk_scores(spark, start_date, end_date)
    risk_df.createOrReplaceTempView("incoming_risk")
    
    risk_count = spark.sql("SELECT COUNT(*) FROM incoming_risk").collect()[0][0]
    print(f"Incoming risk scores: {risk_count}")
    
    if risk_count > 0:
        spark.sql("""
            MERGE INTO s3tablesbucket.gold.risk_scores AS target
            USING incoming_risk AS source
            ON target.stat_date = source.stat_date AND target.entity_id = source.entity_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged risk scores")
    
    # Log high-risk alerts
    high_risk = spark.sql("""
        SELECT entity_id, risk_score, risk_level, evidence
        FROM s3tablesbucket.gold.risk_scores
        WHERE stat_date = CURRENT_DATE() AND alert_triggered = true
        ORDER BY risk_score DESC
        LIMIT 10
    """).collect()
    
    if high_risk:
        print("\n⚠️  HIGH RISK ALERTS:")
        for row in high_risk:
            print(f"  - {row.entity_id}: score={row.risk_score}, {row.evidence}")
    
    print("=" * 60)
    print(f"Gold job completed at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
