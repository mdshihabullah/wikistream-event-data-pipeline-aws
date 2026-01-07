#!/usr/bin/env python3
"""
WikiStream Bronze Layer - Data Quality Gate Job
================================================
Runs DQ checks on Bronze layer data before Silver processing.

This job:
1. Loads recent Bronze data (last 2 hours by default)
2. Runs completeness, timeliness, validity, and uniqueness checks
3. Writes results to dq_audit.quality_results table
4. Publishes metrics to CloudWatch
5. Sends SNS alert on failure
6. Exits with code 1 if gate fails (blocks pipeline)

Runs on EMR Serverless, triggered by Step Functions.
Resource allocation: 2 vCPU driver + 2 vCPU executor = 4 vCPU total
"""

import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

# Import DQ modules
from dq.dq_checks import BronzeDQChecks
from dq.dq_utils import (
    DQAuditWriter,
    DQMetricsPublisher,
    DQAlertManager,
    DataProfiler,
    generate_run_id,
)


# =============================================================================
# Configuration
# =============================================================================

DEFAULT_LOOKBACK_HOURS = 2  # Check recent data only for performance
PROFILE_COLUMNS = ["event_type", "domain", "wiki", "namespace"]
NUMERIC_PROFILE_COLUMNS = ["length_delta", "event_hour"]


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session() -> SparkSession:
    """Create SparkSession for EMR Serverless with S3 Tables Catalog."""
    return (
        SparkSession.builder
        .appName("WikiStream-Bronze-DQ-Gate")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


# =============================================================================
# Main
# =============================================================================

def main():
    """Main entry point for Bronze DQ gate job."""
    print("=" * 60)
    print("WikiStream Bronze DQ Gate")
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
        # Load Bronze data
        print("\n" + "=" * 50)
        print("LOADING BRONZE DATA")
        print("=" * 50)
        
        bronze_df = spark.sql(f"""
            SELECT * FROM s3tablesbucket.bronze.raw_events
            WHERE event_date >= '{start_date}' AND event_date <= '{end_date}'
        """)
        
        record_count = bronze_df.count()
        print(f"Bronze records in window: {record_count}")
        
        if record_count == 0:
            print("⚠️  No Bronze data to check - skipping DQ gate")
            # Still write a result indicating no data
            spark.stop()
            sys.exit(0)  # Exit success - no data is not a failure
        
        # Run DQ checks
        print("\n" + "=" * 50)
        print("RUNNING DQ CHECKS")
        print("=" * 50)
        
        dq_checks = BronzeDQChecks(spark)
        gate_result = dq_checks.run_all_checks(bronze_df, run_id)
        
        # Print results
        print("\n--- DQ Check Results ---")
        for check in gate_result.checks:
            status_icon = "✅" if check.status.value == "PASSED" else (
                "⚠️" if check.status.value == "WARNING" else "❌"
            )
            print(f"  {status_icon} {check.check_name}: {check.status.value}")
            if check.message:
                print(f"      └─ {check.message}")
        
        print(f"\n--- Gate Summary ---")
        print(f"  Total Checks: {gate_result.total_checks}")
        print(f"  Passed: {gate_result.passed_checks}")
        print(f"  Failed: {gate_result.failed_checks}")
        print(f"  Warnings: {gate_result.warning_checks}")
        print(f"  Gate Status: {'✅ PASSED' if gate_result.passed else '❌ FAILED'}")
        
        # Write audit results
        print("\n" + "=" * 50)
        print("WRITING AUDIT RESULTS")
        print("=" * 50)
        
        audit_writer = DQAuditWriter(spark)
        audit_writer.write_results(gate_result)
        
        # Data profiling for drift detection
        print("\n" + "=" * 50)
        print("DATA PROFILING")
        print("=" * 50)
        
        profiler = DataProfiler(spark)
        profiles = profiler.profile_dataframe(
            bronze_df,
            columns=PROFILE_COLUMNS + NUMERIC_PROFILE_COLUMNS,
            numeric_columns=NUMERIC_PROFILE_COLUMNS
        )
        profiler.save_profiles(profiles, run_id, "bronze", "bronze.raw_events")
        
        # Publish CloudWatch metrics
        print("\n" + "=" * 50)
        print("PUBLISHING METRICS")
        print("=" * 50)
        
        metrics_publisher = DQMetricsPublisher()
        metrics_publisher.publish_gate_result(gate_result)
        
        # Send alert if failed
        if not gate_result.passed and sns_topic_arn:
            print("\n" + "=" * 50)
            print("SENDING FAILURE ALERT")
            print("=" * 50)
            
            alert_manager = DQAlertManager(sns_topic_arn)
            alert_manager.send_gate_failure_alert(gate_result)
        
        # Summary
        print("\n" + "=" * 60)
        print("BRONZE DQ GATE COMPLETE")
        print(f"Completed at: {datetime.utcnow().isoformat()}")
        print(f"Gate Status: {'PASSED' if gate_result.passed else 'FAILED'}")
        print("=" * 60)
        
        spark.stop()
        
        # Exit with appropriate code
        if gate_result.passed:
            sys.exit(0)
        else:
            print("\n🚨 Bronze DQ gate FAILED - blocking downstream processing")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Error running Bronze DQ gate: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()

