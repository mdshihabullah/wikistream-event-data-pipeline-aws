#!/usr/bin/env python3
"""
WikiStream Data Quality Utilities Module
=========================================
Utility functions for DQ audit logging, metrics publishing, and alerting.

This module provides:
- DQAuditWriter: Writes DQ results to Iceberg audit table
- DQMetricsPublisher: Publishes metrics to CloudWatch
- DQAlertManager: Sends SNS alerts on DQ failures
- Data profiling utilities for drift detection
"""

import json
import uuid
import boto3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, TimestampType
)
from pyspark.sql.functions import (
    col, lit, current_timestamp, count, avg, stddev,
    min as spark_min, max as spark_max, when, isnull,
    countDistinct, percentile_approx
)

from .dq_checks import DQGateResult, DQCheckResult, CheckStatus


# =============================================================================
# Constants
# =============================================================================

AWS_REGION = "us-east-1"
CLOUDWATCH_NAMESPACE = "WikiStream/DataQuality"
DQ_AUDIT_TABLE = "s3tablesbucket.dq_audit.quality_results"
PROFILE_METRICS_TABLE = "s3tablesbucket.dq_audit.profile_metrics"


# =============================================================================
# Utility Functions
# =============================================================================

def generate_run_id() -> str:
    """Generate a unique run ID for this DQ execution."""
    return f"dq-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"


def get_run_date() -> str:
    """Get current date in YYYY-MM-DD format for partitioning."""
    return datetime.utcnow().strftime("%Y-%m-%d")


# =============================================================================
# DQ Audit Schema
# =============================================================================

DQ_AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("check_type", StringType(), False),
    StructField("status", StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("threshold_value", DoubleType(), True),
    StructField("records_checked", LongType(), True),
    StructField("records_passed", LongType(), True),
    StructField("records_failed", LongType(), True),
    StructField("failure_rate", DoubleType(), True),
    StructField("message", StringType(), True),
    StructField("evidence", StringType(), True),
    StructField("check_duration_ms", LongType(), True),
    StructField("run_timestamp", TimestampType(), False),
    StructField("run_date", StringType(), False),
])

PROFILE_METRICS_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("record_count", LongType(), True),
    StructField("null_count", LongType(), True),
    StructField("null_rate", DoubleType(), True),
    StructField("distinct_count", LongType(), True),
    StructField("mean_value", DoubleType(), True),
    StructField("stddev_value", DoubleType(), True),
    StructField("min_value", DoubleType(), True),
    StructField("max_value", DoubleType(), True),
    StructField("percentile_25", DoubleType(), True),
    StructField("percentile_50", DoubleType(), True),
    StructField("percentile_75", DoubleType(), True),
    StructField("percentile_95", DoubleType(), True),
    StructField("run_timestamp", TimestampType(), False),
    StructField("run_date", StringType(), False),
])


# =============================================================================
# DQ Audit Writer
# =============================================================================

class DQAuditWriter:
    """
    Writes DQ check results to Iceberg audit table for traceability.
    
    The audit table provides:
    - Historical record of all DQ checks
    - Evidence for compliance and debugging
    - Trend analysis of data quality over time
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._ensure_table_exists()
    
    def _ensure_table_exists(self):
        """Create DQ audit table if it doesn't exist."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {DQ_AUDIT_TABLE} (
                run_id STRING NOT NULL,
                layer STRING NOT NULL,
                table_name STRING NOT NULL,
                check_name STRING NOT NULL,
                check_type STRING NOT NULL,
                status STRING NOT NULL,
                metric_value DOUBLE,
                threshold_value DOUBLE,
                records_checked BIGINT,
                records_passed BIGINT,
                records_failed BIGINT,
                failure_rate DOUBLE,
                message STRING,
                evidence STRING,
                check_duration_ms BIGINT,
                run_timestamp TIMESTAMP NOT NULL,
                run_date STRING NOT NULL
            )
            USING iceberg
            PARTITIONED BY (run_date, layer)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'zstd',
                'format-version' = '2'
            )
        """)
    
    def write_results(self, gate_result: DQGateResult) -> None:
        """
        Write all check results from a gate to the audit table.
        
        Args:
            gate_result: DQGateResult containing all check results
        """
        if not gate_result.checks:
            print(f"No checks to write for {gate_result.layer}")
            return
        
        run_date = gate_result.run_timestamp.strftime("%Y-%m-%d")
        
        # Convert check results to rows
        rows = []
        for check in gate_result.checks:
            row = Row(
                run_id=gate_result.run_id,
                layer=gate_result.layer,
                table_name=gate_result.table_name,
                check_name=check.check_name,
                check_type=check.check_type.value,
                status=check.status.value,
                metric_value=float(check.metric_value) if check.metric_value is not None else None,
                threshold_value=float(check.threshold_value) if check.threshold_value is not None else None,
                records_checked=int(check.records_checked) if check.records_checked else None,
                records_passed=int(check.records_passed) if check.records_passed else None,
                records_failed=int(check.records_failed) if check.records_failed else None,
                failure_rate=float(check.failure_rate) if check.failure_rate is not None else None,
                message=check.message,
                evidence=json.dumps(check.evidence) if check.evidence else "{}",
                check_duration_ms=int(check.check_duration_ms) if check.check_duration_ms else None,
                run_timestamp=gate_result.run_timestamp,
                run_date=run_date
            )
            rows.append(row)
        
        # Create DataFrame and write
        results_df = self.spark.createDataFrame(rows, schema=DQ_AUDIT_SCHEMA)
        
        results_df.writeTo(DQ_AUDIT_TABLE).append()
        
        print(f"✅ Wrote {len(rows)} DQ check results to {DQ_AUDIT_TABLE}")
    
    def get_recent_gate_status(
        self, 
        layer: str, 
        lookback_hours: int = 1
    ) -> Optional[bool]:
        """
        Check if the most recent gate for a layer passed.
        
        Args:
            layer: Layer name (bronze, silver, gold)
            lookback_hours: How far back to look for recent runs
        
        Returns:
            True if passed, False if failed, None if no recent runs
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=lookback_hours)
            
            result = self.spark.sql(f"""
                SELECT 
                    run_id,
                    MAX(CASE WHEN status = 'FAILED' OR status = 'ERROR' THEN 1 ELSE 0 END) as has_failure
                FROM {DQ_AUDIT_TABLE}
                WHERE layer = '{layer}'
                  AND run_timestamp >= '{cutoff_time.isoformat()}'
                GROUP BY run_id
                ORDER BY run_id DESC
                LIMIT 1
            """).collect()
            
            if not result:
                return None
            
            return result[0]["has_failure"] == 0
        except Exception as e:
            print(f"Warning: Could not check recent gate status: {e}")
            return None


# =============================================================================
# Data Profiler
# =============================================================================

@dataclass
class ColumnProfile:
    """Profile statistics for a single column."""
    column_name: str
    record_count: int = 0
    null_count: int = 0
    null_rate: float = 0.0
    distinct_count: int = 0
    mean_value: Optional[float] = None
    stddev_value: Optional[float] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    percentile_25: Optional[float] = None
    percentile_50: Optional[float] = None
    percentile_75: Optional[float] = None
    percentile_95: Optional[float] = None


class DataProfiler:
    """
    Computes statistical profiles of data for drift detection.
    
    Profiles include:
    - Completeness metrics (null rates)
    - Cardinality (distinct counts)
    - Distribution statistics (mean, stddev, percentiles)
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._ensure_table_exists()
    
    def _ensure_table_exists(self):
        """Create profile metrics table if it doesn't exist."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {PROFILE_METRICS_TABLE} (
                run_id STRING NOT NULL,
                layer STRING NOT NULL,
                table_name STRING NOT NULL,
                column_name STRING NOT NULL,
                record_count BIGINT,
                null_count BIGINT,
                null_rate DOUBLE,
                distinct_count BIGINT,
                mean_value DOUBLE,
                stddev_value DOUBLE,
                min_value DOUBLE,
                max_value DOUBLE,
                percentile_25 DOUBLE,
                percentile_50 DOUBLE,
                percentile_75 DOUBLE,
                percentile_95 DOUBLE,
                run_timestamp TIMESTAMP NOT NULL,
                run_date STRING NOT NULL
            )
            USING iceberg
            PARTITIONED BY (run_date, layer)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'zstd',
                'format-version' = '2'
            )
        """)
    
    def profile_column(
        self, 
        df: DataFrame, 
        column_name: str,
        is_numeric: bool = False
    ) -> ColumnProfile:
        """
        Compute profile statistics for a single column.
        
        Args:
            df: DataFrame to profile
            column_name: Column to profile
            is_numeric: If True, compute numeric statistics
        
        Returns:
            ColumnProfile with computed statistics
        """
        total_count = df.count()
        if total_count == 0:
            return ColumnProfile(column_name=column_name)
        
        # Basic metrics
        null_count = df.filter(col(column_name).isNull()).count()
        distinct_count = df.select(column_name).distinct().count()
        
        profile = ColumnProfile(
            column_name=column_name,
            record_count=total_count,
            null_count=null_count,
            null_rate=null_count / total_count if total_count > 0 else 0,
            distinct_count=distinct_count
        )
        
        # Numeric statistics
        if is_numeric:
            try:
                stats = df.select(
                    avg(col(column_name)).alias("mean"),
                    stddev(col(column_name)).alias("stddev"),
                    spark_min(col(column_name)).alias("min"),
                    spark_max(col(column_name)).alias("max")
                ).collect()[0]
                
                profile.mean_value = float(stats["mean"]) if stats["mean"] is not None else None
                profile.stddev_value = float(stats["stddev"]) if stats["stddev"] is not None else None
                profile.min_value = float(stats["min"]) if stats["min"] is not None else None
                profile.max_value = float(stats["max"]) if stats["max"] is not None else None
                
                # Percentiles
                percentiles = df.select(
                    percentile_approx(col(column_name), 0.25).alias("p25"),
                    percentile_approx(col(column_name), 0.50).alias("p50"),
                    percentile_approx(col(column_name), 0.75).alias("p75"),
                    percentile_approx(col(column_name), 0.95).alias("p95")
                ).collect()[0]
                
                profile.percentile_25 = float(percentiles["p25"]) if percentiles["p25"] is not None else None
                profile.percentile_50 = float(percentiles["p50"]) if percentiles["p50"] is not None else None
                profile.percentile_75 = float(percentiles["p75"]) if percentiles["p75"] is not None else None
                profile.percentile_95 = float(percentiles["p95"]) if percentiles["p95"] is not None else None
            except Exception as e:
                print(f"Warning: Could not compute numeric stats for {column_name}: {e}")
        
        return profile
    
    def profile_dataframe(
        self,
        df: DataFrame,
        columns: List[str],
        numeric_columns: Optional[List[str]] = None
    ) -> Dict[str, ColumnProfile]:
        """
        Profile multiple columns in a DataFrame.
        
        Args:
            df: DataFrame to profile
            columns: List of columns to profile
            numeric_columns: List of numeric columns for full statistics
        
        Returns:
            Dictionary mapping column names to profiles
        """
        numeric_columns = numeric_columns or []
        profiles = {}
        
        for col_name in columns:
            is_numeric = col_name in numeric_columns
            profiles[col_name] = self.profile_column(df, col_name, is_numeric)
        
        return profiles
    
    def save_profiles(
        self,
        profiles: Dict[str, ColumnProfile],
        run_id: str,
        layer: str,
        table_name: str
    ) -> None:
        """Save profile metrics to the metrics table."""
        if not profiles:
            return
        
        run_timestamp = datetime.utcnow()
        run_date = run_timestamp.strftime("%Y-%m-%d")
        
        rows = []
        for col_name, profile in profiles.items():
            row = Row(
                run_id=run_id,
                layer=layer,
                table_name=table_name,
                column_name=col_name,
                record_count=profile.record_count,
                null_count=profile.null_count,
                null_rate=profile.null_rate,
                distinct_count=profile.distinct_count,
                mean_value=profile.mean_value,
                stddev_value=profile.stddev_value,
                min_value=profile.min_value,
                max_value=profile.max_value,
                percentile_25=profile.percentile_25,
                percentile_50=profile.percentile_50,
                percentile_75=profile.percentile_75,
                percentile_95=profile.percentile_95,
                run_timestamp=run_timestamp,
                run_date=run_date
            )
            rows.append(row)
        
        profiles_df = self.spark.createDataFrame(rows, schema=PROFILE_METRICS_SCHEMA)
        profiles_df.writeTo(PROFILE_METRICS_TABLE).append()
        
        print(f"✅ Saved {len(rows)} profile metrics to {PROFILE_METRICS_TABLE}")
    
    def get_baseline_profile(
        self,
        layer: str,
        table_name: str,
        column_name: str,
        lookback_days: int = 7
    ) -> Optional[ColumnProfile]:
        """
        Get baseline profile statistics from the last N days.
        
        Used for drift detection by comparing current batch to historical baseline.
        """
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            
            result = self.spark.sql(f"""
                SELECT 
                    AVG(null_rate) as avg_null_rate,
                    STDDEV(null_rate) as stddev_null_rate,
                    AVG(record_count) as avg_record_count,
                    STDDEV(record_count) as stddev_record_count,
                    AVG(mean_value) as avg_mean,
                    AVG(stddev_value) as avg_stddev
                FROM {PROFILE_METRICS_TABLE}
                WHERE layer = '{layer}'
                  AND table_name = '{table_name}'
                  AND column_name = '{column_name}'
                  AND run_date >= '{cutoff_date}'
            """).collect()
            
            if not result or result[0]["avg_null_rate"] is None:
                return None
            
            row = result[0]
            return ColumnProfile(
                column_name=column_name,
                null_rate=float(row["avg_null_rate"]) if row["avg_null_rate"] else 0,
                record_count=int(row["avg_record_count"]) if row["avg_record_count"] else 0,
                mean_value=float(row["avg_mean"]) if row["avg_mean"] else None,
                stddev_value=float(row["avg_stddev"]) if row["avg_stddev"] else None
            )
        except Exception as e:
            print(f"Warning: Could not get baseline profile: {e}")
            return None


# =============================================================================
# CloudWatch Metrics Publisher
# =============================================================================

class DQMetricsPublisher:
    """
    Publishes DQ metrics to CloudWatch for monitoring and alerting.
    
    Metrics published:
    - DQGatePassed/Failed: Gate-level pass/fail counts
    - DQCheckStatus: Individual check pass/fail
    - DQPassRate: Percentage of checks passing
    - DQCheckDuration: Check execution time
    """
    
    def __init__(self, region: str = AWS_REGION):
        self.cloudwatch = boto3.client("cloudwatch", region_name=region)
        self.namespace = CLOUDWATCH_NAMESPACE
    
    def publish_gate_result(self, gate_result: DQGateResult) -> None:
        """
        Publish metrics for a complete gate result.
        
        Args:
            gate_result: DQGateResult to publish metrics for
        """
        try:
            metrics = []
            
            # Gate-level metrics
            gate_status = 1 if gate_result.passed else 0
            metrics.append({
                "MetricName": "DQGateStatus",
                "Value": gate_status,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "Layer", "Value": gate_result.layer},
                    {"Name": "Table", "Value": gate_result.table_name}
                ]
            })
            
            # Pass rate
            if gate_result.total_checks > 0:
                pass_rate = (gate_result.passed_checks / gate_result.total_checks) * 100
                metrics.append({
                    "MetricName": "DQPassRate",
                    "Value": pass_rate,
                    "Unit": "Percent",
                    "Dimensions": [
                        {"Name": "Layer", "Value": gate_result.layer}
                    ]
                })
            
            # Individual check metrics
            for check in gate_result.checks:
                check_status = 1 if check.status == CheckStatus.PASSED else 0
                metrics.append({
                    "MetricName": "DQCheckStatus",
                    "Value": check_status,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "Layer", "Value": gate_result.layer},
                        {"Name": "CheckName", "Value": check.check_name[:250]},
                        {"Name": "CheckType", "Value": check.check_type.value}
                    ]
                })
                
                # Duration metrics
                if check.check_duration_ms:
                    metrics.append({
                        "MetricName": "DQCheckDurationMs",
                        "Value": check.check_duration_ms,
                        "Unit": "Milliseconds",
                        "Dimensions": [
                            {"Name": "Layer", "Value": gate_result.layer},
                            {"Name": "CheckName", "Value": check.check_name[:250]}
                        ]
                    })
            
            # Publish in batches of 20 (CloudWatch limit)
            for i in range(0, len(metrics), 20):
                batch = metrics[i:i+20]
                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )
            
            print(f"✅ Published {len(metrics)} DQ metrics to CloudWatch")
            
        except Exception as e:
            print(f"⚠️  Failed to publish CloudWatch metrics: {e}")


# =============================================================================
# SNS Alert Manager
# =============================================================================

class DQAlertManager:
    """
    Sends SNS alerts when DQ gates fail.
    
    Alerts include:
    - Summary of failed checks
    - Evidence/details for debugging
    - Links to audit table for investigation
    """
    
    def __init__(self, sns_topic_arn: str, region: str = AWS_REGION):
        self.sns = boto3.client("sns", region_name=region)
        self.topic_arn = sns_topic_arn
    
    def send_gate_failure_alert(self, gate_result: DQGateResult) -> None:
        """
        Send SNS alert for a failed DQ gate.
        
        Args:
            gate_result: DQGateResult that failed
        """
        if gate_result.passed:
            return  # No alert needed
        
        try:
            # Build failure summary
            failed_checks = [c for c in gate_result.checks if c.is_critical_failure()]
            
            subject = f"🚨 WikiStream DQ Gate FAILED: {gate_result.layer.upper()}"
            
            message = {
                "alert_type": "DATA_QUALITY_GATE_FAILURE",
                "severity": "CRITICAL",
                "timestamp": gate_result.run_timestamp.isoformat() + "Z",
                "run_id": gate_result.run_id,
                "layer": gate_result.layer,
                "table": gate_result.table_name,
                "summary": {
                    "total_checks": gate_result.total_checks,
                    "passed": gate_result.passed_checks,
                    "failed": gate_result.failed_checks,
                    "warnings": gate_result.warning_checks
                },
                "failed_checks": [
                    {
                        "name": c.check_name,
                        "type": c.check_type.value,
                        "message": c.message,
                        "metric_value": c.metric_value,
                        "threshold": c.threshold_value
                    }
                    for c in failed_checks[:5]  # Limit to first 5
                ],
                "action_required": "Investigate failed checks in dq_audit.quality_results table",
                "query_hint": f"SELECT * FROM s3tablesbucket.dq_audit.quality_results WHERE run_id = '{gate_result.run_id}'"
            }
            
            self.sns.publish(
                TopicArn=self.topic_arn,
                Subject=subject[:100],  # SNS subject limit
                Message=json.dumps(message, indent=2, default=str)
            )
            
            print(f"🚨 Sent DQ failure alert to {self.topic_arn}")
            
        except Exception as e:
            print(f"⚠️  Failed to send SNS alert: {e}")
    
    def send_drift_alert(
        self,
        layer: str,
        table_name: str,
        column_name: str,
        current_value: float,
        baseline_value: float,
        z_score: float
    ) -> None:
        """
        Send alert for detected data drift.
        
        Args:
            layer: Data layer
            table_name: Table name
            column_name: Column with drift
            current_value: Current metric value
            baseline_value: Baseline metric value
            z_score: Z-score indicating drift magnitude
        """
        try:
            subject = f"⚠️ WikiStream Data Drift Detected: {layer}/{column_name}"
            
            message = {
                "alert_type": "DATA_DRIFT_DETECTED",
                "severity": "WARNING",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "layer": layer,
                "table": table_name,
                "column": column_name,
                "drift_details": {
                    "current_value": current_value,
                    "baseline_value": baseline_value,
                    "z_score": round(z_score, 2),
                    "deviation": f"{abs(z_score):.1f} standard deviations from baseline"
                },
                "action": "Review data for potential issues or schema changes"
            }
            
            self.sns.publish(
                TopicArn=self.topic_arn,
                Subject=subject[:100],
                Message=json.dumps(message, indent=2, default=str)
            )
            
            print(f"⚠️ Sent drift alert for {layer}/{column_name}")
            
        except Exception as e:
            print(f"⚠️  Failed to send drift alert: {e}")

