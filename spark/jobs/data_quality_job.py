#!/usr/bin/env python3
"""
WikiStream Data Quality Job
===========================
Runs data quality checks on Bronze, Silver, and Gold tables.
Publishes metrics to CloudWatch and alerts on failures via SNS.

This job runs on EMR Serverless, triggered by Step Functions.

NOTE: PyDeequ may have compatibility issues with EMR Serverless.
This implementation includes a fallback to SQL-based checks if Deequ fails.
Optimized for 4 vCPU (1 driver + 1 executor × 2 vCPU)
"""

import os
import sys
import json
import boto3
from datetime import datetime

# ============================================================================
# IMPORTANT: Set SPARK_VERSION before importing PyDeequ modules
# EMR 7.12.0 uses Spark 3.5.x - PyDeequ requires this environment variable
# ============================================================================
if "SPARK_VERSION" not in os.environ:
    os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when, lit

# Try to import Deequ, fall back to SQL-based checks if unavailable
DEEQU_AVAILABLE = False
try:
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationSuite, VerificationResult
    from pydeequ.analyzers import (
        Size, Completeness, Uniqueness, Distinctness,
        Mean, StandardDeviation, Minimum, Maximum
    )
    DEEQU_AVAILABLE = True
    print(f"PyDeequ imported successfully with SPARK_VERSION={os.environ.get('SPARK_VERSION')}")
except ImportError as e:
    print(f"PyDeequ not available, using SQL-based checks: {e}")


# =============================================================================
# Configuration
# =============================================================================

AWS_REGION = "us-east-1"
SNS_TOPIC_ARN = ""  # Set via environment or argument
CLOUDWATCH_NAMESPACE = "WikiStream/DataQuality"


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session() -> SparkSession:
    """Create SparkSession with S3 Tables Catalog support.
    
    Note: Deequ package is loaded via spark-submit parameters.
    """
    return (
        SparkSession.builder
        .appName("WikiStream-DataQuality")
        # Adaptive query execution for better performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Iceberg extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Iceberg timestamp handling
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


# =============================================================================
# Data Quality Checks
# =============================================================================

def check_bronze_quality_sql(spark, verification_results: list):
    """SQL-based Bronze quality checks (fallback when Deequ unavailable)."""
    print("\n" + "=" * 50)
    print("BRONZE TABLE QUALITY CHECKS (SQL-based)")
    print("=" * 50)
    
    try:
        # Get recent data (last 2 hours for incremental processing)
        bronze_df = spark.sql("""
            SELECT * FROM s3tablesbucket.bronze.raw_events
            WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
        """)
        
        row_count = bronze_df.count()
        print(f"Bronze rows (recent): {row_count}")
        
        if row_count == 0:
            print("⚠️  No recent data in Bronze table")
            verification_results.append({
                "table": "bronze.raw_events",
                "check": "row_count",
                "status": "WARNING",
                "message": "No recent data",
                "value": 0
            })
            return
        
        # Check 1: Row count > 0
        verification_results.append({
            "table": "bronze.raw_events",
            "check": "hasSize",
            "status": "PASSED",
            "message": f"Table has {row_count} rows",
            "value": row_count
        })
        print(f"  ✅ hasSize: PASSED ({row_count} rows)")
        
        # Check 2: event_id completeness
        null_event_ids = spark.sql("""
            SELECT COUNT(*) as cnt FROM s3tablesbucket.bronze.raw_events
            WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1) AND event_id IS NULL
        """).collect()[0].cnt
        
        if null_event_ids == 0:
            verification_results.append({
                "table": "bronze.raw_events",
                "check": "isComplete(event_id)",
                "status": "PASSED",
                "message": "All event_ids present"
            })
            print("  ✅ isComplete(event_id): PASSED")
        else:
            verification_results.append({
                "table": "bronze.raw_events",
                "check": "isComplete(event_id)",
                "status": "FAILED",
                "message": f"{null_event_ids} null event_ids"
            })
            print(f"  ❌ isComplete(event_id): FAILED ({null_event_ids} nulls)")
        
        # Check 3: event_id uniqueness
        dup_count = spark.sql("""
            SELECT COUNT(*) as cnt FROM (
                SELECT event_id, COUNT(*) as c 
                FROM s3tablesbucket.bronze.raw_events
                WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
                GROUP BY event_id HAVING COUNT(*) > 1
            )
        """).collect()[0].cnt
        
        if dup_count == 0:
            verification_results.append({
                "table": "bronze.raw_events",
                "check": "isUnique(event_id)",
                "status": "PASSED",
                "message": "All event_ids unique"
            })
            print("  ✅ isUnique(event_id): PASSED")
        else:
            verification_results.append({
                "table": "bronze.raw_events",
                "check": "isUnique(event_id)",
                "status": "FAILED",
                "message": f"{dup_count} duplicate event_ids"
            })
            print(f"  ❌ isUnique(event_id): FAILED ({dup_count} duplicates)")
        
        # Check 4: event_type completeness and validity
        invalid_types = spark.sql("""
            SELECT COUNT(*) as cnt FROM s3tablesbucket.bronze.raw_events
            WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
              AND (event_type IS NULL OR event_type NOT IN ('edit', 'new', 'log', 'categorize', 'external'))
        """).collect()[0].cnt
        
        if invalid_types == 0:
            verification_results.append({
                "table": "bronze.raw_events",
                "check": "isContainedIn(event_type)",
                "status": "PASSED",
                "message": "All event_types valid"
            })
            print("  ✅ isContainedIn(event_type): PASSED")
        else:
            # Allow some invalid types (data from external sources)
            pct = (invalid_types / row_count) * 100
            if pct < 5:  # Less than 5% is acceptable
                verification_results.append({
                    "table": "bronze.raw_events",
                    "check": "isContainedIn(event_type)",
                    "status": "PASSED",
                    "message": f"{invalid_types} unknown types ({pct:.1f}%)"
                })
                print(f"  ✅ isContainedIn(event_type): PASSED ({pct:.1f}% unknown)")
            else:
                verification_results.append({
                    "table": "bronze.raw_events",
                    "check": "isContainedIn(event_type)",
                    "status": "FAILED",
                    "message": f"{invalid_types} invalid ({pct:.1f}%)"
                })
                print(f"  ❌ isContainedIn(event_type): FAILED ({pct:.1f}% invalid)")
                
    except Exception as e:
        print(f"❌ Bronze check failed: {e}")
        verification_results.append({
            "table": "bronze.raw_events",
            "check": "execution",
            "status": "ERROR",
            "message": str(e)
        })


def check_bronze_quality(spark, verification_results: list):
    """
    Run data quality checks on Bronze table.
    Uses PyDeequ if available, falls back to SQL checks.
    """
    if not DEEQU_AVAILABLE:
        return check_bronze_quality_sql(spark, verification_results)
    
    print("\n" + "=" * 50)
    print("BRONZE TABLE QUALITY CHECKS (Deequ)")
    print("=" * 50)
    
    try:
        bronze_df = spark.sql("""
            SELECT * FROM s3tablesbucket.bronze.raw_events
            WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
        """)
        
        row_count = bronze_df.count()
        print(f"Bronze rows (recent): {row_count}")
        
        if row_count == 0:
            print("⚠️  No recent data in Bronze table")
            verification_results.append({
                "table": "bronze.raw_events",
                "check": "row_count",
                "status": "WARNING",
                "message": "No recent data",
                "value": 0
            })
            return
        
        # Define checks
        check = (
            Check(spark, CheckLevel.Error, "Bronze Quality Checks")
            .hasSize(lambda x: x > 0, "Table must have data")
            .isComplete("event_id")
            .isUnique("event_id")
            .isComplete("event_type")
            .isComplete("domain")
            .isComplete("event_timestamp")
            .isContainedIn("event_type", ["edit", "new", "log", "categorize", "external"])
        )
        
        # Run verification
        result = VerificationSuite(spark).onData(bronze_df).addCheck(check).run()
        
        # Process results
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        
        for row in result_df.collect():
            status = "PASSED" if row.constraint_status == "Success" else "FAILED"
            verification_results.append({
                "table": "bronze.raw_events",
                "check": row.constraint,
                "status": status,
                "message": row.constraint_message or "",
                "value": row_count
            })
            print(f"  {'✅' if status == 'PASSED' else '❌'} {row.constraint}: {status}")
        
    except Exception as e:
        print(f"⚠️  Deequ check failed, falling back to SQL: {e}")
        check_bronze_quality_sql(spark, verification_results)


def check_silver_quality_sql(spark, verification_results: list):
    """SQL-based Silver quality checks (fallback when Deequ unavailable)."""
    print("\n" + "=" * 50)
    print("SILVER TABLE QUALITY CHECKS (SQL-based)")
    print("=" * 50)
    
    try:
        row_count = spark.sql("""
            SELECT COUNT(*) as cnt FROM s3tablesbucket.silver.cleaned_events
            WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
        """).collect()[0].cnt
        
        print(f"Silver rows (recent): {row_count}")
        
        if row_count == 0:
            print("⚠️  No recent data in Silver table")
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "row_count",
                "status": "WARNING",
                "message": "No recent data",
                "value": 0
            })
            return
        
        # Check 1: Has data
        verification_results.append({
            "table": "silver.cleaned_events",
            "check": "hasSize",
            "status": "PASSED",
            "message": f"Table has {row_count} rows",
            "value": row_count
        })
        print(f"  ✅ hasSize: PASSED ({row_count} rows)")
        
        # Check 2: event_id uniqueness (critical for dedup verification)
        dup_count = spark.sql("""
            SELECT COUNT(*) as cnt FROM (
                SELECT event_id FROM s3tablesbucket.silver.cleaned_events
                WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
                GROUP BY event_id HAVING COUNT(*) > 1
            )
        """).collect()[0].cnt
        
        if dup_count == 0:
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "isUnique(event_id)",
                "status": "PASSED",
                "message": "All event_ids unique"
            })
            print("  ✅ isUnique(event_id): PASSED")
        else:
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "isUnique(event_id)",
                "status": "FAILED",
                "message": f"{dup_count} duplicate event_ids"
            })
            print(f"  ❌ isUnique(event_id): FAILED ({dup_count} duplicates)")
        
        # Check 3: region values
        invalid_regions = spark.sql("""
            SELECT COUNT(*) as cnt FROM s3tablesbucket.silver.cleaned_events
            WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
              AND region NOT IN ('asia_pacific', 'europe', 'americas', 'middle_east', 'other')
        """).collect()[0].cnt
        
        if invalid_regions == 0:
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "isContainedIn(region)",
                "status": "PASSED",
                "message": "All regions valid"
            })
            print("  ✅ isContainedIn(region): PASSED")
        else:
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "isContainedIn(region)",
                "status": "FAILED",
                "message": f"{invalid_regions} invalid regions"
            })
            print(f"  ❌ isContainedIn(region): FAILED ({invalid_regions} invalid)")
        
        # Check 4: is_valid should be true for all
        invalid_records = spark.sql("""
            SELECT COUNT(*) as cnt FROM s3tablesbucket.silver.cleaned_events
            WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1) AND is_valid != true
        """).collect()[0].cnt
        
        if invalid_records == 0:
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "satisfies(is_valid)",
                "status": "PASSED",
                "message": "All records valid"
            })
            print("  ✅ satisfies(is_valid): PASSED")
        else:
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "satisfies(is_valid)",
                "status": "FAILED",
                "message": f"{invalid_records} invalid records"
            })
            print(f"  ❌ satisfies(is_valid): FAILED ({invalid_records} invalid)")
            
    except Exception as e:
        print(f"❌ Silver check failed: {e}")
        verification_results.append({
            "table": "silver.cleaned_events",
            "check": "execution",
            "status": "ERROR",
            "message": str(e)
        })


def check_silver_quality(spark, verification_results: list):
    """
    Run data quality checks on Silver table.
    Uses PyDeequ if available, falls back to SQL checks.
    """
    if not DEEQU_AVAILABLE:
        return check_silver_quality_sql(spark, verification_results)
    
    print("\n" + "=" * 50)
    print("SILVER TABLE QUALITY CHECKS (Deequ)")
    print("=" * 50)
    
    try:
        silver_df = spark.sql("""
            SELECT * FROM s3tablesbucket.silver.cleaned_events
            WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
        """)
        
        row_count = silver_df.count()
        print(f"Silver rows (recent): {row_count}")
        
        if row_count == 0:
            print("⚠️  No recent data in Silver table")
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "row_count",
                "status": "WARNING",
                "message": "No recent data",
                "value": 0
            })
            return
        
        # Define checks
        check = (
            Check(spark, CheckLevel.Error, "Silver Quality Checks")
            .hasSize(lambda x: x > 0, "Table must have data")
            .isComplete("event_id")
            .isUnique("event_id")  # Must be unique after dedup
            .isComplete("domain")
            .isComplete("region")
            .isComplete("language")
            .isContainedIn("region", ["asia_pacific", "europe", "americas", "middle_east", "other"])
            .satisfies("is_valid = true", "All records should be valid", lambda x: x == 1.0)
        )
        
        result = VerificationSuite(spark).onData(silver_df).addCheck(check).run()
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        
        for row in result_df.collect():
            status = "PASSED" if row.constraint_status == "Success" else "FAILED"
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": row.constraint,
                "status": status,
                "message": row.constraint_message or "",
                "value": row_count
            })
            print(f"  {'✅' if status == 'PASSED' else '❌'} {row.constraint}: {status}")
        
        # Additional: Check deduplication effectiveness
        dup_check = spark.sql("""
            SELECT event_id, COUNT(*) as cnt
            FROM s3tablesbucket.silver.cleaned_events
            WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
            GROUP BY event_id
            HAVING COUNT(*) > 1
        """).count()
        
        if dup_check > 0:
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "no_duplicates",
                "status": "FAILED",
                "message": f"Found {dup_check} duplicate event_ids",
                "value": dup_check
            })
            print(f"  ❌ Duplicate check: FAILED ({dup_check} duplicates)")
        else:
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "no_duplicates",
                "status": "PASSED",
                "message": "No duplicates found",
                "value": 0
            })
            print("  ✅ Duplicate check: PASSED")
        
    except Exception as e:
        print(f"⚠️  Deequ check failed, falling back to SQL: {e}")
        check_silver_quality_sql(spark, verification_results)


def check_gold_quality_sql(spark, verification_results: list):
    """SQL-based Gold quality checks (fallback when Deequ unavailable)."""
    print("\n" + "=" * 50)
    print("GOLD TABLE QUALITY CHECKS (SQL-based)")
    print("=" * 50)
    
    # Check hourly_stats
    try:
        row_count = spark.sql("""
            SELECT COUNT(*) as cnt FROM s3tablesbucket.gold.hourly_stats
            WHERE stat_date >= DATE_SUB(CURRENT_DATE(), 1)
        """).collect()[0].cnt
        
        print(f"Gold hourly_stats rows (recent): {row_count}")
        
        if row_count > 0:
            verification_results.append({
                "table": "gold.hourly_stats",
                "check": "hasSize",
                "status": "PASSED",
                "message": f"Table has {row_count} rows"
            })
            print(f"  ✅ hasSize: PASSED ({row_count} rows)")
            
            # Check for negative values
            invalid = spark.sql("""
                SELECT COUNT(*) as cnt FROM s3tablesbucket.gold.hourly_stats
                WHERE stat_date >= DATE_SUB(CURRENT_DATE(), 1)
                  AND (total_events < 0 OR unique_users < 0 
                       OR bot_percentage < 0 OR bot_percentage > 100)
            """).collect()[0].cnt
            
            if invalid == 0:
                verification_results.append({
                    "table": "gold.hourly_stats",
                    "check": "value_ranges",
                    "status": "PASSED",
                    "message": "All values in valid ranges"
                })
                print("  ✅ value_ranges: PASSED")
            else:
                verification_results.append({
                    "table": "gold.hourly_stats",
                    "check": "value_ranges",
                    "status": "FAILED",
                    "message": f"{invalid} rows with invalid values"
                })
                print(f"  ❌ value_ranges: FAILED ({invalid} invalid)")
        else:
            verification_results.append({
                "table": "gold.hourly_stats",
                "check": "hasSize",
                "status": "WARNING",
                "message": "No recent data"
            })
            print("  ⚠️  hasSize: WARNING (no recent data)")
            
    except Exception as e:
        print(f"❌ Gold hourly_stats check failed: {e}")
        verification_results.append({
            "table": "gold.hourly_stats",
            "check": "execution",
            "status": "ERROR",
            "message": str(e)
        })
    
    # Check risk_scores
    try:
        row_count = spark.sql("""
            SELECT COUNT(*) as cnt FROM s3tablesbucket.gold.risk_scores
            WHERE stat_date >= DATE_SUB(CURRENT_DATE(), 1)
        """).collect()[0].cnt
        
        print(f"Gold risk_scores rows (recent): {row_count}")
        
        if row_count > 0:
            verification_results.append({
                "table": "gold.risk_scores",
                "check": "hasSize",
                "status": "PASSED",
                "message": f"Table has {row_count} rows"
            })
            print(f"  ✅ hasSize: PASSED ({row_count} rows)")
            
            # Check risk_score range
            invalid = spark.sql("""
                SELECT COUNT(*) as cnt FROM s3tablesbucket.gold.risk_scores
                WHERE stat_date >= DATE_SUB(CURRENT_DATE(), 1)
                  AND (risk_score < 0 OR risk_score > 100
                       OR risk_level NOT IN ('LOW', 'MEDIUM', 'HIGH'))
            """).collect()[0].cnt
            
            if invalid == 0:
                verification_results.append({
                    "table": "gold.risk_scores",
                    "check": "risk_score_range",
                    "status": "PASSED",
                    "message": "All scores in valid range [0-100]"
                })
                print("  ✅ risk_score_range: PASSED")
            else:
                verification_results.append({
                    "table": "gold.risk_scores",
                    "check": "risk_score_range",
                    "status": "FAILED",
                    "message": f"{invalid} rows with invalid scores"
                })
                print(f"  ❌ risk_score_range: FAILED ({invalid} invalid)")
        else:
            verification_results.append({
                "table": "gold.risk_scores",
                "check": "hasSize",
                "status": "WARNING",
                "message": "No recent data"
            })
            print("  ⚠️  hasSize: WARNING (no recent data)")
            
    except Exception as e:
        print(f"❌ Gold risk_scores check failed: {e}")
        verification_results.append({
            "table": "gold.risk_scores",
            "check": "execution",
            "status": "ERROR",
            "message": str(e)
        })


def check_gold_quality(spark, verification_results: list):
    """
    Run data quality checks on Gold tables.
    Uses PyDeequ if available, falls back to SQL checks.
    """
    if not DEEQU_AVAILABLE:
        return check_gold_quality_sql(spark, verification_results)
    
    print("\n" + "=" * 50)
    print("GOLD TABLE QUALITY CHECKS (Deequ)")
    print("=" * 50)
    
    # Check hourly_stats
    try:
        stats_df = spark.sql("""
            SELECT * FROM s3tablesbucket.gold.hourly_stats
            WHERE stat_date >= DATE_SUB(CURRENT_DATE(), 1)
        """)
        
        row_count = stats_df.count()
        print(f"Gold hourly_stats rows (recent): {row_count}")
        
        if row_count > 0:
            check = (
                Check(spark, CheckLevel.Error, "Gold Stats Quality")
                .hasSize(lambda x: x > 0)
                .isComplete("domain")
                .isNonNegative("total_events")
                .isNonNegative("unique_users")
                .satisfies("bot_percentage >= 0 AND bot_percentage <= 100", 
                          "Bot percentage in valid range")
            )
            
            result = VerificationSuite(spark).onData(stats_df).addCheck(check).run()
            result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
            
            for row in result_df.collect():
                status = "PASSED" if row.constraint_status == "Success" else "FAILED"
                verification_results.append({
                    "table": "gold.hourly_stats",
                    "check": row.constraint,
                    "status": status,
                    "message": row.constraint_message or ""
                })
                print(f"  {'✅' if status == 'PASSED' else '❌'} {row.constraint}: {status}")
    except Exception as e:
        print(f"⚠️  Deequ check failed, falling back to SQL: {e}")
        check_gold_quality_sql(spark, verification_results)
        return
    
    # Check risk_scores
    try:
        risk_df = spark.sql("""
            SELECT * FROM s3tablesbucket.gold.risk_scores
            WHERE stat_date >= DATE_SUB(CURRENT_DATE(), 1)
        """)
        
        row_count = risk_df.count()
        print(f"Gold risk_scores rows (recent): {row_count}")
        
        if row_count > 0:
            check = (
                Check(spark, CheckLevel.Error, "Gold Risk Quality")
                .hasSize(lambda x: x > 0)
                .isComplete("entity_id")
                .isComplete("risk_score")
                .satisfies("risk_score >= 0 AND risk_score <= 100", 
                          "Risk score in valid range")
                .isContainedIn("risk_level", ["LOW", "MEDIUM", "HIGH"])
            )
            
            result = VerificationSuite(spark).onData(risk_df).addCheck(check).run()
            result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
            
            for row in result_df.collect():
                status = "PASSED" if row.constraint_status == "Success" else "FAILED"
                verification_results.append({
                    "table": "gold.risk_scores",
                    "check": row.constraint,
                    "status": status,
                    "message": row.constraint_message or ""
                })
                print(f"  {'✅' if status == 'PASSED' else '❌'} {row.constraint}: {status}")
    except Exception as e:
        print(f"❌ Gold risk_scores check failed: {e}")
        verification_results.append({
            "table": "gold.risk_scores",
            "check": "execution",
            "status": "ERROR",
            "message": str(e)
        })


# =============================================================================
# Metrics & Alerting
# =============================================================================

def publish_metrics(verification_results: list):
    """Publish data quality metrics to CloudWatch."""
    print("\n" + "=" * 50)
    print("PUBLISHING METRICS TO CLOUDWATCH")
    print("=" * 50)
    
    try:
        cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
        
        metrics = []
        for result in verification_results:
            # Count by status
            metric_value = 1 if result["status"] == "PASSED" else 0
            
            metrics.append({
                "MetricName": "DataQualityCheckStatus",
                "Dimensions": [
                    {"Name": "Table", "Value": result["table"]},
                    {"Name": "Check", "Value": result["check"][:250]},  # Truncate
                ],
                "Value": metric_value,
                "Unit": "Count"
            })
        
        # Publish in batches of 20
        for i in range(0, len(metrics), 20):
            batch = metrics[i:i+20]
            cloudwatch.put_metric_data(
                Namespace=CLOUDWATCH_NAMESPACE,
                MetricData=batch
            )
        
        print(f"Published {len(metrics)} metrics")
        
    except Exception as e:
        print(f"Failed to publish metrics: {e}")


def send_alerts(verification_results: list, sns_topic_arn: str):
    """Send alerts for failed checks via SNS."""
    failures = [r for r in verification_results if r["status"] in ["FAILED", "ERROR"]]
    
    if not failures:
        print("\n✅ All checks passed - no alerts needed")
        return
    
    print(f"\n⚠️  {len(failures)} checks failed - sending alert")
    
    if not sns_topic_arn:
        print("SNS topic ARN not configured, skipping alert")
        return
    
    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        
        message = {
            "alert_type": "DATA_QUALITY_FAILURE",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "failure_count": len(failures),
            "failures": failures[:10],  # First 10 failures
            "summary": f"{len(failures)} data quality checks failed"
        }
        
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject="WikiStream Data Quality Alert",
            Message=json.dumps(message, indent=2)
        )
        
        print(f"Alert sent to {sns_topic_arn}")
        
    except Exception as e:
        print(f"Failed to send alert: {e}")


# =============================================================================
# Main
# =============================================================================

def main():
    """Main entry point for data quality job."""
    print("=" * 60)
    print("WikiStream Data Quality Job")
    print(f"Started at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    # Parse arguments
    global SNS_TOPIC_ARN
    if len(sys.argv) >= 2:
        SNS_TOPIC_ARN = sys.argv[1]
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Collect all verification results
    verification_results = []
    
    # Run checks on each layer
    check_bronze_quality(spark, verification_results)
    check_silver_quality(spark, verification_results)
    check_gold_quality(spark, verification_results)
    
    # Publish metrics
    publish_metrics(verification_results)
    
    # Send alerts if needed
    send_alerts(verification_results, SNS_TOPIC_ARN)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    passed = len([r for r in verification_results if r["status"] == "PASSED"])
    failed = len([r for r in verification_results if r["status"] == "FAILED"])
    errors = len([r for r in verification_results if r["status"] == "ERROR"])
    warnings = len([r for r in verification_results if r["status"] == "WARNING"])
    
    print(f"  ✅ Passed:   {passed}")
    print(f"  ❌ Failed:   {failed}")
    print(f"  💥 Errors:   {errors}")
    print(f"  ⚠️  Warnings: {warnings}")
    
    print(f"\nCompleted at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    spark.stop()
    
    # Exit with error code if any failures
    if failed > 0 or errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
