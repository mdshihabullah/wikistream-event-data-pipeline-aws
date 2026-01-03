#!/usr/bin/env python3
"""
WikiStream Data Quality Job
===========================
Runs Deequ data quality checks on Bronze, Silver, and Gold tables.
Publishes metrics to CloudWatch and alerts on failures via SNS.

This job runs on EMR Serverless, triggered by Step Functions.
"""

import sys
import json
import boto3
from datetime import datetime

from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import (
    Size, Completeness, Uniqueness, Distinctness,
    Mean, StandardDeviation, Minimum, Maximum
)


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
    """Create SparkSession with Deequ and S3 Tables Catalog support."""
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
        # S3 Tables Catalog (config passed via spark-submit)
        # Deequ requires
        .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5")
        .getOrCreate()
    )


# =============================================================================
# Data Quality Checks
# =============================================================================

def check_bronze_quality(spark, verification_results: list):
    """
    Run data quality checks on Bronze table.
    
    Checks:
    - Completeness of required fields
    - Uniqueness of event_id
    - Data freshness
    """
    print("\n" + "=" * 50)
    print("BRONZE TABLE QUALITY CHECKS")
    print("=" * 50)
    
    try:
        bronze_df = spark.sql("""
            SELECT * FROM s3tablesbucket.bronze.raw_events
            WHERE event_date = CURRENT_DATE()
        """)
        
        row_count = bronze_df.count()
        print(f"Bronze rows (today): {row_count}")
        
        if row_count == 0:
            print("⚠️  No data in Bronze table for today")
            verification_results.append({
                "table": "bronze.raw_events",
                "check": "row_count",
                "status": "WARNING",
                "message": "No data for today",
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
        print(f"❌ Bronze check failed: {e}")
        verification_results.append({
            "table": "bronze.raw_events",
            "check": "execution",
            "status": "ERROR",
            "message": str(e)
        })


def check_silver_quality(spark, verification_results: list):
    """
    Run data quality checks on Silver table.
    
    Checks:
    - Completeness after cleaning
    - No duplicates (deduplication worked)
    - Valid transformations
    """
    print("\n" + "=" * 50)
    print("SILVER TABLE QUALITY CHECKS")
    print("=" * 50)
    
    try:
        silver_df = spark.sql("""
            SELECT * FROM s3tablesbucket.silver.cleaned_events
            WHERE event_date = CURRENT_DATE()
        """)
        
        row_count = silver_df.count()
        print(f"Silver rows (today): {row_count}")
        
        if row_count == 0:
            print("⚠️  No data in Silver table for today")
            verification_results.append({
                "table": "silver.cleaned_events",
                "check": "row_count",
                "status": "WARNING",
                "message": "No data for today",
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
            WHERE event_date = CURRENT_DATE()
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
            print(f"  ✅ Duplicate check: PASSED")
        
    except Exception as e:
        print(f"❌ Silver check failed: {e}")
        verification_results.append({
            "table": "silver.cleaned_events",
            "check": "execution",
            "status": "ERROR",
            "message": str(e)
        })


def check_gold_quality(spark, verification_results: list):
    """
    Run data quality checks on Gold tables.
    
    Checks:
    - Aggregation consistency
    - Risk score validity
    - Data freshness
    """
    print("\n" + "=" * 50)
    print("GOLD TABLE QUALITY CHECKS")
    print("=" * 50)
    
    # Check hourly_stats
    try:
        stats_df = spark.sql("""
            SELECT * FROM s3tablesbucket.gold.hourly_stats
            WHERE stat_date = CURRENT_DATE()
        """)
        
        row_count = stats_df.count()
        print(f"Gold hourly_stats rows (today): {row_count}")
        
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
        print(f"❌ Gold hourly_stats check failed: {e}")
        verification_results.append({
            "table": "gold.hourly_stats",
            "check": "execution",
            "status": "ERROR",
            "message": str(e)
        })
    
    # Check risk_scores
    try:
        risk_df = spark.sql("""
            SELECT * FROM s3tablesbucket.gold.risk_scores
            WHERE stat_date = CURRENT_DATE()
        """)
        
        row_count = risk_df.count()
        print(f"Gold risk_scores rows (today): {row_count}")
        
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
