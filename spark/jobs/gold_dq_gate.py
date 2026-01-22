#!/usr/bin/env python3
"""
WikiStream Gold Layer - Data Quality Gate Job
==============================================
Runs DQ checks on Gold layer data after aggregation.

This job:
1. Verifies upstream gates (Bronze and Silver) passed
2. Loads recent Gold data (hourly_stats and risk_scores)
3. Runs validation and consistency checks
4. Writes results to dq_audit.quality_results table
5. Publishes metrics to CloudWatch
6. Sends SNS alert on failure

Runs on EMR Serverless, triggered by Step Functions.
Resource allocation: 2 vCPU driver + 2 vCPU executor = 4 vCPU total
"""

import os
import sys
import atexit
from datetime import datetime, timedelta

# ============================================================================
# IMPORTANT: Set SPARK_VERSION before importing PyDeequ modules
# EMR 7.12.0 uses Spark 3.5.x - PyDeequ requires this environment variable
# ============================================================================
if "SPARK_VERSION" not in os.environ:
    os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import SparkSession

# Import DQ modules
from dq.dq_checks import GoldDQChecks
from dq.dq_utils import (
    DQAuditWriter,
    DQMetricsPublisher,
    DQAlertManager,
    generate_run_id,
)


# =============================================================================
# Configuration
# =============================================================================

DEFAULT_LOOKBACK_HOURS = 2  # Check recent data only


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session() -> SparkSession:
    """Create SparkSession for EMR Serverless with S3 Tables Catalog."""
    return (
        SparkSession.builder
        .appName("WikiStream-Gold-DQ-Gate")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


# =============================================================================
# Main
# =============================================================================

# Global spark session for cleanup
spark = None

def cleanup_spark():
    """Ensure Spark session is stopped on exit."""
    global spark
    if spark is not None:
        try:
            print("\n[CLEANUP] Stopping Spark session...")
            spark.stop()
            print("[CLEANUP] Spark session stopped")
        except Exception as e:
            print(f"[CLEANUP] Error stopping Spark: {e}")

# Register cleanup function to run on exit
atexit.register(cleanup_spark)


def main():
    """Main entry point for Gold DQ gate job."""
    global spark
    
    print("=" * 60)
    print("WikiStream Gold DQ Gate")
    print(f"Started at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    # Parse arguments
    sns_topic_arn = ""
    lookback_hours = DEFAULT_LOOKBACK_HOURS
    
    if len(sys.argv) >= 2:
        sns_topic_arn = sys.argv[1]
    if len(sys.argv) >= 3:
        lookback_hours = int(sys.argv[2])
    
    print(f"SNS Topic: {sns_topic_arn or 'Not configured'}")
    print(f"Lookback: {lookback_hours} hours")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Generate run ID
    run_id = generate_run_id()
    print(f"Run ID: {run_id}")
    
    # Calculate time window
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=lookback_hours)
    start_date = start_time.strftime("%Y-%m-%d")
    end_date = end_time.strftime("%Y-%m-%d")
    
    print(f"\nProcessing window: {start_date} to {end_date}")
    
    try:
        # Check upstream gate status
        print("\n" + "=" * 50)
        print("CHECKING UPSTREAM GATES")
        print("=" * 50)
        
        audit_writer = DQAuditWriter(spark)
        
        bronze_passed = audit_writer.get_recent_gate_status("bronze", lookback_hours=1)
        silver_passed = audit_writer.get_recent_gate_status("silver", lookback_hours=1)
        
        # Default to True if no recent runs (first time or gaps)
        if bronze_passed is None:
            print("  ⚠️  No recent Bronze gate run found - assuming passed")
            bronze_passed = True
        else:
            print(f"  Bronze gate: {'✅ PASSED' if bronze_passed else '❌ FAILED'}")
        
        if silver_passed is None:
            print("  ⚠️  No recent Silver gate run found - assuming passed")
            silver_passed = True
        else:
            print(f"  Silver gate: {'✅ PASSED' if silver_passed else '❌ FAILED'}")
        
        # Initialize DQ checks
        dq_checks = GoldDQChecks(spark)
        all_results = []
        overall_passed = True
        
        # Load and check hourly_stats
        print("\n" + "=" * 50)
        print("CHECKING GOLD.HOURLY_STATS")
        print("=" * 50)
        
        try:
            hourly_df = spark.sql(f"""
                SELECT * FROM s3tablesbucket.gold.hourly_stats
                WHERE stat_date >= '{start_date}' AND stat_date <= '{end_date}'
            """)
            
            hourly_count = hourly_df.count()
            print(f"Hourly stats records: {hourly_count}")
            
            if hourly_count > 0:
                hourly_result = dq_checks.run_all_checks(
                    hourly_df, 
                    run_id,
                    bronze_passed=bronze_passed,
                    silver_passed=silver_passed
                )
                all_results.append(hourly_result)
                
                # Print results
                print("\n--- Hourly Stats Check Results ---")
                for check in hourly_result.checks:
                    status_icon = "✅" if check.status.value == "PASSED" else (
                        "⚠️" if check.status.value == "WARNING" else "❌"
                    )
                    print(f"  {status_icon} {check.check_name}: {check.status.value}")
                
                if not hourly_result.passed:
                    overall_passed = False
            else:
                print("  ⚠️  No hourly_stats data to check")
                
        except Exception as e:
            print(f"  ❌ Error checking hourly_stats: {e}")
            overall_passed = False
        
        # Load and check risk_scores
        print("\n" + "=" * 50)
        print("CHECKING GOLD.RISK_SCORES")
        print("=" * 50)
        
        try:
            risk_df = spark.sql(f"""
                SELECT * FROM s3tablesbucket.gold.risk_scores
                WHERE stat_date >= '{start_date}' AND stat_date <= '{end_date}'
            """)
            
            risk_count = risk_df.count()
            print(f"Risk score records: {risk_count}")
            
            if risk_count > 0:
                risk_result = dq_checks.run_risk_score_checks(
                    risk_df,
                    run_id,
                    bronze_passed=bronze_passed,
                    silver_passed=silver_passed
                )
                all_results.append(risk_result)
                
                # Print results
                print("\n--- Risk Scores Check Results ---")
                for check in risk_result.checks:
                    status_icon = "✅" if check.status.value == "PASSED" else (
                        "⚠️" if check.status.value == "WARNING" else "❌"
                    )
                    print(f"  {status_icon} {check.check_name}: {check.status.value}")
                
                if not risk_result.passed:
                    overall_passed = False
            else:
                print("  ⚠️  No risk_scores data to check")
                
        except Exception as e:
            print(f"  ❌ Error checking risk_scores: {e}")
            overall_passed = False
        
        # Write audit results
        print("\n" + "=" * 50)
        print("WRITING AUDIT RESULTS")
        print("=" * 50)
        
        for result in all_results:
            audit_writer.write_results(result)
        
        # Publish CloudWatch metrics
        print("\n" + "=" * 50)
        print("PUBLISHING METRICS")
        print("=" * 50)
        
        metrics_publisher = DQMetricsPublisher()
        for result in all_results:
            metrics_publisher.publish_gate_result(result)
        
        # Send alert if failed
        if not overall_passed and sns_topic_arn:
            print("\n" + "=" * 50)
            print("SENDING FAILURE ALERT")
            print("=" * 50)
            
            alert_manager = DQAlertManager(sns_topic_arn)
            for result in all_results:
                if not result.passed:
                    alert_manager.send_gate_failure_alert(result)
        
        # Summary
        print("\n" + "=" * 60)
        print("GOLD DQ GATE COMPLETE")
        print(f"Completed at: {datetime.utcnow().isoformat()}")
        
        total_checks = sum(r.total_checks for r in all_results)
        total_passed = sum(r.passed_checks for r in all_results)
        total_failed = sum(r.failed_checks for r in all_results)
        
        print(f"Total Checks: {total_checks}")
        print(f"Passed: {total_passed}")
        print(f"Failed: {total_failed}")
        print(f"Gate Status: {'PASSED' if overall_passed else 'FAILED'}")
        print("=" * 60)

        # Stop Spark session cleanly
        spark.stop()
        print("[CLEANUP] Spark session stopped, forcing JVM shutdown...")
        
        # Flush output streams before JVM exit
        sys.stdout.flush()
        sys.stderr.flush()

        # Exit with appropriate code
        exit_code = 0 if overall_passed else 1
        print(f"\n🏁 Gold DQ Gate exiting with code: {exit_code}")
        # Force JVM shutdown to signal EMR Serverless job completion
        spark.sparkContext._gateway.jvm.System.exit(exit_code)

    except Exception as e:
        print(f"\n❌ Error running Gold DQ gate: {e}")
        import traceback
        traceback.print_exc()
        # Ensure Spark is stopped even on error
        try:
            spark.stop()
        except Exception as spark_error:
            print(f"Warning: Error stopping Spark: {spark_error}")
        # Force JVM shutdown on error
        sys.stdout.flush()
        sys.stderr.flush()
        spark.sparkContext._gateway.jvm.System.exit(1)


if __name__ == "__main__":
    main()
