#!/usr/bin/env python3
"""
WikiStream Gold Layer - Aggregation & Analytics Job
====================================================
Reads from Silver table, computes aggregations and risk scores for Gold layer.

This job runs on EMR Serverless, triggered by Step Functions on schedule.
Produces tables optimized for QuickSight dashboards:
  - gold.hourly_stats: Hourly metrics by domain/region
  - gold.risk_scores: Entity-level risk scores with evidence
  - gold.daily_analytics_summary: Executive dashboard with KPIs and trends
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

SCHEMA_VERSION = "1.0.0"  # Track schema version for evolution

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
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
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
            
            CURRENT_TIMESTAMP() AS gold_processed_at,
            '{SCHEMA_VERSION}' AS schema_version
            
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
            
            CURRENT_TIMESTAMP() AS gold_processed_at,
            '{SCHEMA_VERSION}' AS schema_version
            
        FROM user_metrics
    """)
    
    return risk_df


def compute_daily_analytics_summary(spark, start_date: str, end_date: str):
    """
    Compute daily executive summary with KPIs, risk overview, and trends.
    
    This single-row-per-day table provides:
    - Platform health overview
    - Risk distribution metrics
    - Content quality indicators
    - Activity trends for dashboards
    """
    print("Computing daily analytics summary...")
    
    summary_df = spark.sql(f"""
        WITH daily_base AS (
            SELECT
                event_date,
                COUNT(*) AS total_events,
                COUNT(DISTINCT user_normalized) AS unique_users,
                COUNT(DISTINCT domain) AS active_domains,
                COUNT(DISTINCT title) AS unique_pages_edited,
                
                -- User segmentation
                SUM(CASE WHEN is_bot = true THEN 1 ELSE 0 END) AS bot_events,
                SUM(CASE WHEN is_anonymous = true THEN 1 ELSE 0 END) AS anonymous_events,
                SUM(CASE WHEN is_bot = false AND is_anonymous = false THEN 1 ELSE 0 END) AS registered_user_events,
                
                -- Content metrics
                SUM(CASE WHEN length_delta > 0 THEN length_delta ELSE 0 END) AS total_bytes_added,
                SUM(CASE WHEN length_delta < 0 THEN ABS(length_delta) ELSE 0 END) AS total_bytes_removed,
                AVG(ABS(COALESCE(length_delta, 0))) AS avg_edit_size_bytes,
                
                -- Edit type breakdown
                SUM(CASE WHEN event_type = 'edit' THEN 1 ELSE 0 END) AS edit_events,
                SUM(CASE WHEN event_type = 'new' THEN 1 ELSE 0 END) AS new_page_events,
                
                -- Quality flags
                SUM(CASE WHEN is_large_deletion = true THEN 1 ELSE 0 END) AS large_deletions_count,
                SUM(CASE WHEN is_large_addition = true THEN 1 ELSE 0 END) AS large_additions_count,
                
                -- Regional distribution
                SUM(CASE WHEN region = 'europe' THEN 1 ELSE 0 END) AS europe_events,
                SUM(CASE WHEN region = 'americas' THEN 1 ELSE 0 END) AS americas_events,
                SUM(CASE WHEN region = 'asia_pacific' THEN 1 ELSE 0 END) AS asia_pacific_events,
                
                -- Peak hour activity
                MAX(hour_events) AS peak_hour_events
            FROM (
                SELECT 
                    event_date,
                    user_normalized,
                    domain,
                    title,
                    is_bot,
                    is_anonymous,
                    length_delta,
                    event_type,
                    is_large_deletion,
                    is_large_addition,
                    region,
                    COUNT(*) OVER (PARTITION BY event_date, HOUR(event_timestamp)) AS hour_events
                FROM s3tablesbucket.silver.cleaned_events
                WHERE event_date >= '{start_date}' AND event_date <= '{end_date}'
            )
            GROUP BY event_date
        ),
        risk_summary AS (
            SELECT
                stat_date,
                COUNT(*) AS total_scored_users,
                SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_users,
                SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk_users,
                SUM(CASE WHEN risk_level = 'LOW' THEN 1 ELSE 0 END) AS low_risk_users,
                AVG(risk_score) AS avg_risk_score,
                MAX(risk_score) AS max_risk_score,
                SUM(CASE WHEN alert_triggered = true THEN 1 ELSE 0 END) AS alerts_triggered
            FROM s3tablesbucket.gold.risk_scores
            WHERE stat_date >= '{start_date}' AND stat_date <= '{end_date}'
            GROUP BY stat_date
        )
        SELECT
            d.event_date AS summary_date,
            
            -- Volume KPIs
            d.total_events,
            d.unique_users,
            d.active_domains,
            d.unique_pages_edited,
            
            -- User Mix (percentages)
            ROUND(100.0 * d.bot_events / NULLIF(d.total_events, 0), 2) AS bot_percentage,
            ROUND(100.0 * d.anonymous_events / NULLIF(d.total_events, 0), 2) AS anonymous_percentage,
            ROUND(100.0 * d.registered_user_events / NULLIF(d.total_events, 0), 2) AS registered_user_percentage,
            
            -- Content Health
            d.total_bytes_added,
            d.total_bytes_removed,
            d.total_bytes_added - d.total_bytes_removed AS net_content_change,
            ROUND(d.avg_edit_size_bytes, 2) AS avg_edit_size_bytes,
            d.new_page_events AS new_pages_created,
            
            -- Quality Indicators
            d.large_deletions_count,
            d.large_additions_count,
            ROUND(100.0 * d.large_deletions_count / NULLIF(d.total_events, 0), 4) AS large_deletion_rate,
            
            -- Risk Overview
            COALESCE(r.high_risk_users, 0) AS high_risk_user_count,
            COALESCE(r.medium_risk_users, 0) AS medium_risk_user_count,
            COALESCE(r.low_risk_users, 0) AS low_risk_user_count,
            COALESCE(r.avg_risk_score, 0) AS platform_avg_risk_score,
            COALESCE(r.max_risk_score, 0) AS platform_max_risk_score,
            COALESCE(r.alerts_triggered, 0) AS total_alerts_triggered,
            
            -- Regional Distribution (percentages)
            ROUND(100.0 * d.europe_events / NULLIF(d.total_events, 0), 2) AS europe_percentage,
            ROUND(100.0 * d.americas_events / NULLIF(d.total_events, 0), 2) AS americas_percentage,
            ROUND(100.0 * d.asia_pacific_events / NULLIF(d.total_events, 0), 2) AS asia_pacific_percentage,
            
            -- Activity Pattern
            d.peak_hour_events,
            ROUND(d.total_events / 24.0, 2) AS avg_events_per_hour,
            
            -- Health Score (0-100): Higher is better
            -- Based on: low risk ratio, registered user ratio, content growth
            ROUND(LEAST(100, GREATEST(0,
                -- Low risk user bonus (0-40)
                40 * COALESCE(r.low_risk_users, 0) / NULLIF(COALESCE(r.total_scored_users, 1), 0)
                -- Registered user bonus (0-30)
                + 30 * d.registered_user_events / NULLIF(d.total_events, 1)
                -- Content growth bonus (0-20)
                + CASE 
                    WHEN d.total_bytes_added > d.total_bytes_removed THEN 20
                    WHEN d.total_bytes_added > d.total_bytes_removed * 0.5 THEN 10
                    ELSE 0
                END
                -- Low deletion rate bonus (0-10)
                + CASE 
                    WHEN d.large_deletions_count < d.total_events * 0.01 THEN 10
                    WHEN d.large_deletions_count < d.total_events * 0.05 THEN 5
                    ELSE 0
                END
            )), 2) AS platform_health_score,
            
            CURRENT_TIMESTAMP() AS gold_processed_at,
            '{SCHEMA_VERSION}' AS schema_version
            
        FROM daily_base d
        LEFT JOIN risk_summary r ON d.event_date = r.stat_date
    """)
    
    return summary_df


# =============================================================================
# Main
# =============================================================================

def main():
    """Main entry point for Gold batch job.
    
    Optimized for ≤5 minute SLA:
    - Default lookback: 1 hour (process recent Silver data quickly)
    - Incremental aggregation: only new Silver records
    - Resource efficient: 4 vCPU total (1 driver + 1 executor × 2 vCPU)
    """
    print("=" * 60)
    print("WikiStream Gold Batch Job")
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
            gold_processed_at TIMESTAMP,
            schema_version STRING
        )
        USING iceberg
        PARTITIONED BY (stat_date, region)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '268435456',
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
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
            gold_processed_at TIMESTAMP,
            schema_version STRING
        )
        USING iceberg
        PARTITIONED BY (stat_date)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '268435456',
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
    """)
    
    # Daily analytics summary table - Executive dashboard KPIs
    spark.sql("""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.gold.daily_analytics_summary (
            summary_date STRING NOT NULL,
            
            -- Volume KPIs
            total_events BIGINT,
            unique_users BIGINT,
            active_domains BIGINT,
            unique_pages_edited BIGINT,
            
            -- User Mix (percentages)
            bot_percentage DOUBLE,
            anonymous_percentage DOUBLE,
            registered_user_percentage DOUBLE,
            
            -- Content Health
            total_bytes_added BIGINT,
            total_bytes_removed BIGINT,
            net_content_change BIGINT,
            avg_edit_size_bytes DOUBLE,
            new_pages_created BIGINT,
            
            -- Quality Indicators
            large_deletions_count BIGINT,
            large_additions_count BIGINT,
            large_deletion_rate DOUBLE,
            
            -- Risk Overview
            high_risk_user_count BIGINT,
            medium_risk_user_count BIGINT,
            low_risk_user_count BIGINT,
            platform_avg_risk_score DOUBLE,
            platform_max_risk_score DOUBLE,
            total_alerts_triggered BIGINT,
            
            -- Regional Distribution
            europe_percentage DOUBLE,
            americas_percentage DOUBLE,
            asia_pacific_percentage DOUBLE,
            
            -- Activity Pattern
            peak_hour_events BIGINT,
            avg_events_per_hour DOUBLE,
            
            -- Overall Health
            platform_health_score DOUBLE,
            
            gold_processed_at TIMESTAMP,
            schema_version STRING
        )
        USING iceberg
        PARTITIONED BY (summary_date)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '268435456',
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
    """)
    
    # Add schema_version column if it doesn't exist (for existing tables)
    try:
        spark.sql("ALTER TABLE s3tablesbucket.gold.hourly_stats ADD COLUMN schema_version STRING")
        print("Added schema_version column to gold.hourly_stats")
    except Exception:
        pass
    
    try:
        spark.sql("ALTER TABLE s3tablesbucket.gold.risk_scores ADD COLUMN schema_version STRING")
        print("Added schema_version column to gold.risk_scores")
    except Exception:
        pass
    
    # Compute and write hourly stats
    hourly_df = compute_hourly_stats(spark, start_date, end_date)
    hourly_df.createOrReplaceTempView("incoming_hourly")
    
    hourly_count = spark.sql("SELECT COUNT(*) FROM incoming_hourly").collect()[0][0]
    print(f"Incoming hourly stats: {hourly_count}")
    
    if hourly_count > 0:
        # MERGE with partition columns (stat_date, region) + domain for unique key
        # Note: Iceberg requires explicit column lists, not UPDATE SET * or INSERT *
        spark.sql("""
            MERGE INTO s3tablesbucket.gold.hourly_stats AS target
            USING incoming_hourly AS source
            ON target.stat_date = source.stat_date 
               AND target.region = source.region
               AND target.stat_hour = source.stat_hour 
               AND target.domain = source.domain
            WHEN MATCHED THEN UPDATE SET
                target.stat_date = source.stat_date,
                target.stat_hour = source.stat_hour,
                target.domain = source.domain,
                target.region = source.region,
                target.total_events = source.total_events,
                target.unique_users = source.unique_users,
                target.unique_pages = source.unique_pages,
                target.bytes_added = source.bytes_added,
                target.bytes_removed = source.bytes_removed,
                target.avg_edit_size = source.avg_edit_size,
                target.bot_edits = source.bot_edits,
                target.human_edits = source.human_edits,
                target.bot_percentage = source.bot_percentage,
                target.anonymous_edits = source.anonymous_edits,
                target.type_edit = source.type_edit,
                target.type_new = source.type_new,
                target.type_categorize = source.type_categorize,
                target.type_log = source.type_log,
                target.large_deletions = source.large_deletions,
                target.large_additions = source.large_additions,
                target.gold_processed_at = source.gold_processed_at,
                target.schema_version = source.schema_version
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged hourly stats")
    
    # Compute and write risk scores
    risk_df = compute_risk_scores(spark, start_date, end_date)
    risk_df.createOrReplaceTempView("incoming_risk")
    
    risk_count = spark.sql("SELECT COUNT(*) FROM incoming_risk").collect()[0][0]
    print(f"Incoming risk scores: {risk_count}")
    
    if risk_count > 0:
        # MERGE with explicit columns
        # Note: Iceberg requires explicit column lists, not UPDATE SET * or INSERT *
        spark.sql("""
            MERGE INTO s3tablesbucket.gold.risk_scores AS target
            USING incoming_risk AS source
            ON target.stat_date = source.stat_date AND target.entity_id = source.entity_id
            WHEN MATCHED THEN UPDATE SET
                target.stat_date = source.stat_date,
                target.entity_id = source.entity_id,
                target.entity_type = source.entity_type,
                target.total_edits = source.total_edits,
                target.edits_per_hour_avg = source.edits_per_hour_avg,
                target.large_deletions = source.large_deletions,
                target.domains_edited = source.domains_edited,
                target.risk_score = source.risk_score,
                target.risk_level = source.risk_level,
                target.evidence = source.evidence,
                target.alert_triggered = source.alert_triggered,
                target.gold_processed_at = source.gold_processed_at,
                target.schema_version = source.schema_version
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged risk scores")
    
    # Compute and write daily analytics summary (after risk scores are available)
    summary_df = compute_daily_analytics_summary(spark, start_date, end_date)
    summary_df.createOrReplaceTempView("incoming_summary")
    
    summary_count = spark.sql("SELECT COUNT(*) FROM incoming_summary").collect()[0][0]
    print(f"Incoming daily summaries: {summary_count}")
    
    if summary_count > 0:
        spark.sql("""
            MERGE INTO s3tablesbucket.gold.daily_analytics_summary AS target
            USING incoming_summary AS source
            ON target.summary_date = source.summary_date
            WHEN MATCHED THEN UPDATE SET
                target.summary_date = source.summary_date,
                target.total_events = source.total_events,
                target.unique_users = source.unique_users,
                target.active_domains = source.active_domains,
                target.unique_pages_edited = source.unique_pages_edited,
                target.bot_percentage = source.bot_percentage,
                target.anonymous_percentage = source.anonymous_percentage,
                target.registered_user_percentage = source.registered_user_percentage,
                target.total_bytes_added = source.total_bytes_added,
                target.total_bytes_removed = source.total_bytes_removed,
                target.net_content_change = source.net_content_change,
                target.avg_edit_size_bytes = source.avg_edit_size_bytes,
                target.new_pages_created = source.new_pages_created,
                target.large_deletions_count = source.large_deletions_count,
                target.large_additions_count = source.large_additions_count,
                target.large_deletion_rate = source.large_deletion_rate,
                target.high_risk_user_count = source.high_risk_user_count,
                target.medium_risk_user_count = source.medium_risk_user_count,
                target.low_risk_user_count = source.low_risk_user_count,
                target.platform_avg_risk_score = source.platform_avg_risk_score,
                target.platform_max_risk_score = source.platform_max_risk_score,
                target.total_alerts_triggered = source.total_alerts_triggered,
                target.europe_percentage = source.europe_percentage,
                target.americas_percentage = source.americas_percentage,
                target.asia_pacific_percentage = source.asia_pacific_percentage,
                target.peak_hour_events = source.peak_hour_events,
                target.avg_events_per_hour = source.avg_events_per_hour,
                target.platform_health_score = source.platform_health_score,
                target.gold_processed_at = source.gold_processed_at,
                target.schema_version = source.schema_version
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged daily analytics summary")
        
        # Print today's health score
        today_summary = spark.sql(f"""
            SELECT platform_health_score, platform_avg_risk_score, total_events, high_risk_user_count
            FROM incoming_summary
            LIMIT 1
        """).collect()
        
        if today_summary:
            row = today_summary[0]
            print(f"\n📊 DAILY PLATFORM SUMMARY:")
            print(f"   Health Score: {row.platform_health_score}/100")
            print(f"   Avg Risk Score: {row.platform_avg_risk_score}")
            print(f"   Total Events: {row.total_events}")
            print(f"   High Risk Users: {row.high_risk_user_count}")
    
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
