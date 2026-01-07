#!/usr/bin/env python3
"""
WikiStream Data Quality Checks Module
======================================
Deequ-based data quality checks for Bronze, Silver, and Gold layers.

This module provides:
- Completeness checks (critical and important fields)
- Timeliness checks (event freshness validation)
- Validity checks (value range and format validation)
- Accuracy checks (cross-field consistency)
- Uniqueness checks (deduplication verification)
- Data drift detection (statistical profile comparison)

All checks follow industry best practices for data quality in streaming pipelines.
"""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, abs as spark_abs,
    unix_timestamp, avg, regexp_extract
)


class CheckStatus(Enum):
    """Data quality check status."""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"


class CheckType(Enum):
    """Data quality check type."""
    COMPLETENESS = "completeness"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    UNIQUENESS = "uniqueness"
    DRIFT = "drift"
    UPSTREAM = "upstream"


@dataclass
class DQCheckResult:
    """Result of a single data quality check."""
    check_name: str
    check_type: CheckType
    status: CheckStatus
    metric_value: Optional[float] = None
    threshold_value: Optional[float] = None
    records_checked: int = 0
    records_passed: int = 0
    records_failed: int = 0
    failure_rate: float = 0.0
    message: str = ""
    evidence: Dict[str, Any] = field(default_factory=dict)
    check_duration_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "check_name": self.check_name,
            "check_type": self.check_type.value,
            "status": self.status.value,
            "metric_value": self.metric_value,
            "threshold_value": self.threshold_value,
            "records_checked": self.records_checked,
            "records_passed": self.records_passed,
            "records_failed": self.records_failed,
            "failure_rate": self.failure_rate,
            "message": self.message,
            "evidence": json.dumps(self.evidence) if self.evidence else "{}",
            "check_duration_ms": self.check_duration_ms,
        }

    def is_passed(self) -> bool:
        """Check if the result is passing (PASSED or WARNING)."""
        return self.status in [CheckStatus.PASSED, CheckStatus.WARNING]

    def is_critical_failure(self) -> bool:
        """Check if the result is a critical failure."""
        return self.status in [CheckStatus.FAILED, CheckStatus.ERROR]


@dataclass
class DQGateResult:
    """Aggregate result of all checks for a layer."""
    layer: str
    table_name: str
    run_id: str
    run_timestamp: datetime
    checks: List[DQCheckResult] = field(default_factory=list)
    
    @property
    def passed(self) -> bool:
        """Returns True if all checks passed (no FAILED or ERROR)."""
        return all(not check.is_critical_failure() for check in self.checks)
    
    @property
    def total_checks(self) -> int:
        return len(self.checks)
    
    @property
    def passed_checks(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.PASSED)
    
    @property
    def failed_checks(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.FAILED)
    
    @property
    def warning_checks(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.WARNING)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all checks."""
        return {
            "layer": self.layer,
            "table_name": self.table_name,
            "run_id": self.run_id,
            "run_timestamp": self.run_timestamp.isoformat(),
            "gate_passed": self.passed,
            "total_checks": self.total_checks,
            "passed": self.passed_checks,
            "failed": self.failed_checks,
            "warnings": self.warning_checks,
        }


class BaseDQChecks(ABC):
    """Base class for data quality checks."""
    
    def __init__(self, spark: SparkSession, layer: str, table_name: str):
        self.spark = spark
        self.layer = layer
        self.table_name = table_name
        self.results: List[DQCheckResult] = []
    
    @abstractmethod
    def run_all_checks(self, df: DataFrame, run_id: str) -> DQGateResult:
        """Run all checks for this layer."""
        pass
    
    def _check_completeness(
        self,
        df: DataFrame,
        column: str,
        threshold: float = 1.0,
        is_critical: bool = True
    ) -> DQCheckResult:
        """
        Check completeness (non-null rate) of a column.
        
        Args:
            df: DataFrame to check
            column: Column name to check
            threshold: Minimum completeness rate (0.0 to 1.0)
            is_critical: If True, FAIL on threshold breach; else WARNING
        """
        start_time = datetime.utcnow()
        
        try:
            total_count = df.count()
            if total_count == 0:
                return DQCheckResult(
                    check_name=f"completeness_{column}",
                    check_type=CheckType.COMPLETENESS,
                    status=CheckStatus.SKIPPED,
                    message="No records to check"
                )
            
            null_count = df.filter(col(column).isNull()).count()
            non_null_count = total_count - null_count
            completeness_rate = non_null_count / total_count
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            if completeness_rate >= threshold:
                status = CheckStatus.PASSED
                message = f"Completeness {completeness_rate:.2%} >= {threshold:.2%}"
            elif completeness_rate >= 0.95 and not is_critical:
                status = CheckStatus.WARNING
                message = f"Completeness {completeness_rate:.2%} below target but >= 95%"
            else:
                status = CheckStatus.FAILED if is_critical else CheckStatus.WARNING
                message = f"Completeness {completeness_rate:.2%} < {threshold:.2%}"
            
            return DQCheckResult(
                check_name=f"completeness_{column}",
                check_type=CheckType.COMPLETENESS,
                status=status,
                metric_value=completeness_rate,
                threshold_value=threshold,
                records_checked=total_count,
                records_passed=non_null_count,
                records_failed=null_count,
                failure_rate=null_count / total_count if total_count > 0 else 0,
                message=message,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name=f"completeness_{column}",
                check_type=CheckType.COMPLETENESS,
                status=CheckStatus.ERROR,
                message=f"Error checking completeness: {str(e)}"
            )
    
    def _check_uniqueness(
        self,
        df: DataFrame,
        column: str,
        threshold: float = 1.0
    ) -> DQCheckResult:
        """
        Check uniqueness of values in a column within the batch.
        
        Args:
            df: DataFrame to check
            column: Column name to check
            threshold: Minimum uniqueness rate (0.0 to 1.0)
        """
        start_time = datetime.utcnow()
        
        try:
            total_count = df.count()
            if total_count == 0:
                return DQCheckResult(
                    check_name=f"uniqueness_{column}",
                    check_type=CheckType.UNIQUENESS,
                    status=CheckStatus.SKIPPED,
                    message="No records to check"
                )
            
            distinct_count = df.select(column).distinct().count()
            duplicate_count = total_count - distinct_count
            uniqueness_rate = distinct_count / total_count
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            # Get sample duplicates for evidence
            evidence = {}
            if duplicate_count > 0:
                dup_sample = (
                    df.groupBy(column)
                    .count()
                    .filter(col("count") > 1)
                    .limit(5)
                    .collect()
                )
                evidence["sample_duplicates"] = [
                    {"value": str(row[column]), "count": row["count"]} 
                    for row in dup_sample
                ]
            
            if uniqueness_rate >= threshold:
                status = CheckStatus.PASSED
                message = f"Uniqueness {uniqueness_rate:.2%} >= {threshold:.2%}"
            else:
                status = CheckStatus.FAILED
                message = f"Found {duplicate_count} duplicate values"
            
            return DQCheckResult(
                check_name=f"uniqueness_{column}",
                check_type=CheckType.UNIQUENESS,
                status=status,
                metric_value=uniqueness_rate,
                threshold_value=threshold,
                records_checked=total_count,
                records_passed=distinct_count,
                records_failed=duplicate_count,
                failure_rate=duplicate_count / total_count if total_count > 0 else 0,
                message=message,
                evidence=evidence,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name=f"uniqueness_{column}",
                check_type=CheckType.UNIQUENESS,
                status=CheckStatus.ERROR,
                message=f"Error checking uniqueness: {str(e)}"
            )
    
    def _check_value_in_set(
        self,
        df: DataFrame,
        column: str,
        allowed_values: List[str],
        threshold: float = 0.95
    ) -> DQCheckResult:
        """
        Check that column values are within an allowed set.
        
        Args:
            df: DataFrame to check
            column: Column name to check
            allowed_values: List of allowed values
            threshold: Minimum valid rate
        """
        start_time = datetime.utcnow()
        
        try:
            total_count = df.count()
            if total_count == 0:
                return DQCheckResult(
                    check_name=f"validity_{column}_in_set",
                    check_type=CheckType.VALIDITY,
                    status=CheckStatus.SKIPPED,
                    message="No records to check"
                )
            
            valid_count = df.filter(col(column).isin(allowed_values)).count()
            invalid_count = total_count - valid_count
            valid_rate = valid_count / total_count
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            # Get sample invalid values for evidence
            evidence = {"allowed_values": allowed_values}
            if invalid_count > 0:
                invalid_sample = (
                    df.filter(~col(column).isin(allowed_values))
                    .select(column)
                    .distinct()
                    .limit(10)
                    .collect()
                )
                evidence["sample_invalid"] = [str(row[column]) for row in invalid_sample]
            
            if valid_rate >= threshold:
                status = CheckStatus.PASSED
                message = f"Validity {valid_rate:.2%} >= {threshold:.2%}"
            else:
                status = CheckStatus.FAILED
                message = f"Found {invalid_count} invalid values ({(1-valid_rate):.2%})"
            
            return DQCheckResult(
                check_name=f"validity_{column}_in_set",
                check_type=CheckType.VALIDITY,
                status=status,
                metric_value=valid_rate,
                threshold_value=threshold,
                records_checked=total_count,
                records_passed=valid_count,
                records_failed=invalid_count,
                failure_rate=invalid_count / total_count if total_count > 0 else 0,
                message=message,
                evidence=evidence,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name=f"validity_{column}_in_set",
                check_type=CheckType.VALIDITY,
                status=CheckStatus.ERROR,
                message=f"Error checking validity: {str(e)}"
            )
    
    def _check_range(
        self,
        df: DataFrame,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        threshold: float = 1.0
    ) -> DQCheckResult:
        """
        Check that numeric column values are within a range.
        """
        start_time = datetime.utcnow()
        
        try:
            # Filter out nulls for range check
            non_null_df = df.filter(col(column).isNotNull())
            total_count = non_null_df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name=f"validity_{column}_range",
                    check_type=CheckType.VALIDITY,
                    status=CheckStatus.SKIPPED,
                    message="No non-null records to check"
                )
            
            # Build filter condition
            condition = lit(True)
            if min_val is not None:
                condition = condition & (col(column) >= min_val)
            if max_val is not None:
                condition = condition & (col(column) <= max_val)
            
            valid_count = non_null_df.filter(condition).count()
            invalid_count = total_count - valid_count
            valid_rate = valid_count / total_count
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            evidence = {}
            if min_val is not None:
                evidence["min_allowed"] = min_val
            if max_val is not None:
                evidence["max_allowed"] = max_val
            
            if valid_rate >= threshold:
                status = CheckStatus.PASSED
                message = f"Range validity {valid_rate:.2%} >= {threshold:.2%}"
            else:
                status = CheckStatus.FAILED
                message = f"Found {invalid_count} out-of-range values"
            
            return DQCheckResult(
                check_name=f"validity_{column}_range",
                check_type=CheckType.VALIDITY,
                status=status,
                metric_value=valid_rate,
                threshold_value=threshold,
                records_checked=total_count,
                records_passed=valid_count,
                records_failed=invalid_count,
                failure_rate=invalid_count / total_count if total_count > 0 else 0,
                message=message,
                evidence=evidence,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name=f"validity_{column}_range",
                check_type=CheckType.VALIDITY,
                status=CheckStatus.ERROR,
                message=f"Error checking range: {str(e)}"
            )


class BronzeDQChecks(BaseDQChecks):
    """
    Bronze Layer Data Quality Checks.
    
    Checks:
    - Completeness: Critical fields (event_id, event_type, domain, event_timestamp)
    - Completeness: Important fields (title, user, wiki) with 95% threshold
    - Timeliness: event_timestamp within 1 minute of bronze_processed_at (95th percentile)
    - Timeliness: kafka_timestamp within 5 minutes of event_timestamp
    - Validity: event_type in allowed set
    - Validity: event_hour in range 0-23
    - Validity: namespace >= 0
    """
    
    # Configuration constants
    CRITICAL_FIELDS = ["event_id", "event_type", "domain", "event_timestamp"]
    IMPORTANT_FIELDS = ["title", "user", "wiki"]
    ALLOWED_EVENT_TYPES = ["edit", "new", "log", "categorize", "external", "unknown"]
    TIMELINESS_THRESHOLD_SECONDS = 60  # 1 minute
    TIMELINESS_PERCENTILE = 0.95  # 95th percentile
    KAFKA_LAG_THRESHOLD_SECONDS = 300  # 5 minutes
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark, "bronze", "bronze.raw_events")
    
    def run_all_checks(self, df: DataFrame, run_id: str) -> DQGateResult:
        """Run all Bronze layer DQ checks."""
        results = []
        
        # 1. Critical field completeness (100%)
        for field in self.CRITICAL_FIELDS:
            result = self._check_completeness(df, field, threshold=1.0, is_critical=True)
            results.append(result)
        
        # 2. Important field completeness (95%)
        for field in self.IMPORTANT_FIELDS:
            result = self._check_completeness(df, field, threshold=0.95, is_critical=False)
            results.append(result)
        
        # 3. Timeliness: event_timestamp vs bronze_processed_at
        timeliness_result = self._check_timeliness(df)
        results.append(timeliness_result)
        
        # 4. Timeliness: kafka_timestamp vs event_timestamp
        kafka_lag_result = self._check_kafka_lag(df)
        results.append(kafka_lag_result)
        
        # 5. Validity: event_type in allowed set
        event_type_result = self._check_value_in_set(
            df, "event_type", self.ALLOWED_EVENT_TYPES, threshold=0.95
        )
        results.append(event_type_result)
        
        # 6. Validity: event_hour in range 0-23
        hour_result = self._check_range(df, "event_hour", min_val=0, max_val=23)
        results.append(hour_result)
        
        # 7. Validity: namespace >= 0 (when not null)
        namespace_result = self._check_range(df, "namespace", min_val=0)
        results.append(namespace_result)
        
        # 8. Uniqueness: event_id within batch
        uniqueness_result = self._check_uniqueness(df, "event_id")
        results.append(uniqueness_result)
        
        return DQGateResult(
            layer=self.layer,
            table_name=self.table_name,
            run_id=run_id,
            run_timestamp=datetime.utcnow(),
            checks=results
        )
    
    def _check_timeliness(self, df: DataFrame) -> DQCheckResult:
        """
        Check timeliness: 95% of records should have event_timestamp 
        within 1 minute of bronze_processed_at.
        """
        start_time = datetime.utcnow()
        
        try:
            # Filter records with both timestamps
            timed_df = df.filter(
                col("event_timestamp").isNotNull() & 
                col("bronze_processed_at").isNotNull()
            )
            total_count = timed_df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name="timeliness_event_vs_processed",
                    check_type=CheckType.TIMELINESS,
                    status=CheckStatus.SKIPPED,
                    message="No records with both timestamps"
                )
            
            # Calculate time difference in seconds
            timed_df = timed_df.withColumn(
                "latency_seconds",
                spark_abs(
                    unix_timestamp("bronze_processed_at") - 
                    unix_timestamp("event_timestamp")
                )
            )
            
            # Check 95th percentile
            percentile_95 = timed_df.selectExpr(
                f"percentile_approx(latency_seconds, {self.TIMELINESS_PERCENTILE})"
            ).collect()[0][0]
            
            # Count records within threshold
            within_threshold = timed_df.filter(
                col("latency_seconds") <= self.TIMELINESS_THRESHOLD_SECONDS
            ).count()
            
            within_threshold_rate = within_threshold / total_count
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            evidence = {
                "percentile_95_seconds": round(percentile_95, 2) if percentile_95 else None,
                "threshold_seconds": self.TIMELINESS_THRESHOLD_SECONDS,
                "records_within_threshold": within_threshold,
                "within_threshold_rate": round(within_threshold_rate, 4)
            }
            
            # Pass if 95th percentile is within threshold
            if percentile_95 is not None and percentile_95 <= self.TIMELINESS_THRESHOLD_SECONDS:
                status = CheckStatus.PASSED
                message = f"95th percentile latency {percentile_95:.1f}s <= {self.TIMELINESS_THRESHOLD_SECONDS}s"
            elif within_threshold_rate >= 0.95:
                status = CheckStatus.PASSED
                message = f"{within_threshold_rate:.1%} records within {self.TIMELINESS_THRESHOLD_SECONDS}s threshold"
            else:
                status = CheckStatus.FAILED
                message = f"Only {within_threshold_rate:.1%} records within threshold"
            
            return DQCheckResult(
                check_name="timeliness_event_vs_processed",
                check_type=CheckType.TIMELINESS,
                status=status,
                metric_value=percentile_95,
                threshold_value=float(self.TIMELINESS_THRESHOLD_SECONDS),
                records_checked=total_count,
                records_passed=within_threshold,
                records_failed=total_count - within_threshold,
                failure_rate=1 - within_threshold_rate,
                message=message,
                evidence=evidence,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="timeliness_event_vs_processed",
                check_type=CheckType.TIMELINESS,
                status=CheckStatus.ERROR,
                message=f"Error checking timeliness: {str(e)}"
            )
    
    def _check_kafka_lag(self, df: DataFrame) -> DQCheckResult:
        """
        Check Kafka ingestion lag: kafka_timestamp vs event_timestamp.
        Warning if >10% exceed 5 minutes.
        """
        start_time = datetime.utcnow()
        
        try:
            # Filter records with both timestamps
            timed_df = df.filter(
                col("kafka_timestamp").isNotNull() & 
                col("event_timestamp").isNotNull()
            )
            total_count = timed_df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name="timeliness_kafka_lag",
                    check_type=CheckType.TIMELINESS,
                    status=CheckStatus.SKIPPED,
                    message="No records with both timestamps"
                )
            
            # Calculate lag in seconds
            timed_df = timed_df.withColumn(
                "kafka_lag_seconds",
                spark_abs(
                    unix_timestamp("kafka_timestamp") - 
                    unix_timestamp("event_timestamp")
                )
            )
            
            # Count records exceeding threshold
            exceeding = timed_df.filter(
                col("kafka_lag_seconds") > self.KAFKA_LAG_THRESHOLD_SECONDS
            ).count()
            
            exceed_rate = exceeding / total_count
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            # Get average lag
            avg_lag = timed_df.agg(avg("kafka_lag_seconds")).collect()[0][0]
            
            evidence = {
                "avg_lag_seconds": round(avg_lag, 2) if avg_lag else None,
                "threshold_seconds": self.KAFKA_LAG_THRESHOLD_SECONDS,
                "exceeding_count": exceeding,
                "exceed_rate": round(exceed_rate, 4)
            }
            
            if exceed_rate <= 0.10:  # Warning threshold
                status = CheckStatus.PASSED
                message = f"Kafka lag acceptable: {(1-exceed_rate):.1%} within {self.KAFKA_LAG_THRESHOLD_SECONDS}s"
            else:
                status = CheckStatus.WARNING
                message = f"{exceed_rate:.1%} records exceed {self.KAFKA_LAG_THRESHOLD_SECONDS}s Kafka lag"
            
            return DQCheckResult(
                check_name="timeliness_kafka_lag",
                check_type=CheckType.TIMELINESS,
                status=status,
                metric_value=avg_lag,
                threshold_value=float(self.KAFKA_LAG_THRESHOLD_SECONDS),
                records_checked=total_count,
                records_passed=total_count - exceeding,
                records_failed=exceeding,
                failure_rate=exceed_rate,
                message=message,
                evidence=evidence,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="timeliness_kafka_lag",
                check_type=CheckType.TIMELINESS,
                status=CheckStatus.ERROR,
                message=f"Error checking Kafka lag: {str(e)}"
            )


class SilverDQChecks(BaseDQChecks):
    """
    Silver Layer Data Quality Checks.
    
    Checks:
    - Accuracy: length_delta == length_new - length_old
    - Accuracy: is_anonymous correctly derived from IP pattern
    - Accuracy: region correctly mapped from domain
    - Consistency: is_valid flag consistency
    - Uniqueness: event_id uniqueness within batch
    - Drift: Record count and null rate vs baseline
    """
    
    # Domain to region mapping for validation
    DOMAIN_REGION_MAP = {
        "zh.wikipedia.org": "asia_pacific",
        "ja.wikipedia.org": "asia_pacific",
        "ko.wikipedia.org": "asia_pacific",
        "vi.wikipedia.org": "asia_pacific",
        "de.wikipedia.org": "europe",
        "fr.wikipedia.org": "europe",
        "it.wikipedia.org": "europe",
        "es.wikipedia.org": "europe",
        "pl.wikipedia.org": "europe",
        "nl.wikipedia.org": "europe",
        "ru.wikipedia.org": "europe",
        "en.wikipedia.org": "americas",
        "pt.wikipedia.org": "americas",
        "ar.wikipedia.org": "middle_east",
        "fa.wikipedia.org": "middle_east",
        "he.wikipedia.org": "middle_east",
    }
    
    # IP address pattern for anonymous detection
    IP_PATTERN = r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"
    
    ALLOWED_REGIONS = ["asia_pacific", "europe", "americas", "middle_east", "other"]
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark, "silver", "silver.cleaned_events")
    
    def run_all_checks(self, df: DataFrame, run_id: str) -> DQGateResult:
        """Run all Silver layer DQ checks."""
        results = []
        
        # 1. Accuracy: length_delta calculation
        delta_result = self._check_length_delta_accuracy(df)
        results.append(delta_result)
        
        # 2. Accuracy: is_anonymous derivation
        anonymous_result = self._check_anonymous_accuracy(df)
        results.append(anonymous_result)
        
        # 3. Accuracy: region mapping
        region_result = self._check_region_mapping(df)
        results.append(region_result)
        
        # 4. Validity: region in allowed set
        region_valid = self._check_value_in_set(df, "region", self.ALLOWED_REGIONS)
        results.append(region_valid)
        
        # 5. Consistency: is_valid flag
        valid_flag_result = self._check_valid_flag_consistency(df)
        results.append(valid_flag_result)
        
        # 6. Uniqueness: event_id
        uniqueness_result = self._check_uniqueness(df, "event_id")
        results.append(uniqueness_result)
        
        # 7. Completeness: Critical fields
        for field in ["event_id", "domain", "region", "event_timestamp"]:
            result = self._check_completeness(df, field, threshold=1.0, is_critical=True)
            results.append(result)
        
        return DQGateResult(
            layer=self.layer,
            table_name=self.table_name,
            run_id=run_id,
            run_timestamp=datetime.utcnow(),
            checks=results
        )
    
    def _check_length_delta_accuracy(self, df: DataFrame) -> DQCheckResult:
        """
        Verify that length_delta == length_new - length_old when both are present.
        """
        start_time = datetime.utcnow()
        
        try:
            # Filter records with all three values
            calc_df = df.filter(
                col("length_old").isNotNull() & 
                col("length_new").isNotNull() & 
                col("length_delta").isNotNull()
            )
            total_count = calc_df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name="accuracy_length_delta",
                    check_type=CheckType.ACCURACY,
                    status=CheckStatus.SKIPPED,
                    message="No records with length values to check"
                )
            
            # Check accuracy: length_delta should equal length_new - length_old
            accurate_count = calc_df.filter(
                col("length_delta") == (col("length_new") - col("length_old"))
            ).count()
            
            inaccurate_count = total_count - accurate_count
            accuracy_rate = accurate_count / total_count
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            if accuracy_rate >= 0.99:  # Allow 1% tolerance
                status = CheckStatus.PASSED
                message = f"length_delta accuracy: {accuracy_rate:.2%}"
            else:
                status = CheckStatus.FAILED
                message = f"length_delta mismatch: {inaccurate_count} records ({(1-accuracy_rate):.2%})"
            
            return DQCheckResult(
                check_name="accuracy_length_delta",
                check_type=CheckType.ACCURACY,
                status=status,
                metric_value=accuracy_rate,
                threshold_value=0.99,
                records_checked=total_count,
                records_passed=accurate_count,
                records_failed=inaccurate_count,
                failure_rate=inaccurate_count / total_count if total_count > 0 else 0,
                message=message,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="accuracy_length_delta",
                check_type=CheckType.ACCURACY,
                status=CheckStatus.ERROR,
                message=f"Error checking length_delta accuracy: {str(e)}"
            )
    
    def _check_anonymous_accuracy(self, df: DataFrame) -> DQCheckResult:
        """
        Verify that is_anonymous is correctly derived from IP pattern in user field.
        """
        start_time = datetime.utcnow()
        
        try:
            # Check records with user field
            user_df = df.filter(col("user_normalized").isNotNull())
            total_count = user_df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name="accuracy_is_anonymous",
                    check_type=CheckType.ACCURACY,
                    status=CheckStatus.SKIPPED,
                    message="No user records to check"
                )
            
            # Derive expected is_anonymous based on IP pattern
            user_df = user_df.withColumn(
                "expected_anonymous",
                when(
                    regexp_extract(col("user_normalized"), self.IP_PATTERN, 0) != "",
                    lit(True)
                ).otherwise(lit(False))
            )
            
            # Count matches
            accurate_count = user_df.filter(
                col("is_anonymous") == col("expected_anonymous")
            ).count()
            
            inaccurate_count = total_count - accurate_count
            accuracy_rate = accurate_count / total_count
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            if accuracy_rate >= 0.99:
                status = CheckStatus.PASSED
                message = f"is_anonymous accuracy: {accuracy_rate:.2%}"
            else:
                status = CheckStatus.FAILED
                message = f"is_anonymous mismatch: {inaccurate_count} records"
            
            return DQCheckResult(
                check_name="accuracy_is_anonymous",
                check_type=CheckType.ACCURACY,
                status=status,
                metric_value=accuracy_rate,
                threshold_value=0.99,
                records_checked=total_count,
                records_passed=accurate_count,
                records_failed=inaccurate_count,
                failure_rate=inaccurate_count / total_count if total_count > 0 else 0,
                message=message,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="accuracy_is_anonymous",
                check_type=CheckType.ACCURACY,
                status=CheckStatus.ERROR,
                message=f"Error checking is_anonymous accuracy: {str(e)}"
            )
    
    def _check_region_mapping(self, df: DataFrame) -> DQCheckResult:
        """
        Verify that region is correctly derived from domain for known domains.
        """
        start_time = datetime.utcnow()
        
        try:
            # Filter to known domains
            known_domains = list(self.DOMAIN_REGION_MAP.keys())
            known_df = df.filter(col("domain").isin(known_domains))
            total_count = known_df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name="accuracy_region_mapping",
                    check_type=CheckType.ACCURACY,
                    status=CheckStatus.SKIPPED,
                    message="No known domain records to check"
                )
            
            # Check each domain-region pair
            correct_count = 0
            for domain, expected_region in self.DOMAIN_REGION_MAP.items():
                count = known_df.filter(
                    (col("domain") == domain) & (col("region") == expected_region)
                ).count()
                correct_count += count
            
            incorrect_count = total_count - correct_count
            accuracy_rate = correct_count / total_count
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            if accuracy_rate >= 1.0:  # Should be 100% for known domains
                status = CheckStatus.PASSED
                message = f"Region mapping 100% correct for known domains"
            else:
                status = CheckStatus.FAILED
                message = f"Region mapping incorrect for {incorrect_count} records"
            
            return DQCheckResult(
                check_name="accuracy_region_mapping",
                check_type=CheckType.ACCURACY,
                status=status,
                metric_value=accuracy_rate,
                threshold_value=1.0,
                records_checked=total_count,
                records_passed=correct_count,
                records_failed=incorrect_count,
                failure_rate=incorrect_count / total_count if total_count > 0 else 0,
                message=message,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="accuracy_region_mapping",
                check_type=CheckType.ACCURACY,
                status=CheckStatus.ERROR,
                message=f"Error checking region mapping: {str(e)}"
            )
    
    def _check_valid_flag_consistency(self, df: DataFrame) -> DQCheckResult:
        """
        Check that is_valid flag is consistent with actual validity checks.
        All Silver records should have is_valid = True (invalid filtered out).
        """
        start_time = datetime.utcnow()
        
        try:
            total_count = df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name="consistency_is_valid",
                    check_type=CheckType.CONSISTENCY,
                    status=CheckStatus.SKIPPED,
                    message="No records to check"
                )
            
            # All Silver records should have is_valid = True
            valid_count = df.filter(col("is_valid") == True).count()
            invalid_count = total_count - valid_count
            valid_rate = valid_count / total_count
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            if valid_rate >= 1.0:
                status = CheckStatus.PASSED
                message = "All Silver records have is_valid = True"
            else:
                status = CheckStatus.FAILED
                message = f"Found {invalid_count} records with is_valid != True"
            
            return DQCheckResult(
                check_name="consistency_is_valid",
                check_type=CheckType.CONSISTENCY,
                status=status,
                metric_value=valid_rate,
                threshold_value=1.0,
                records_checked=total_count,
                records_passed=valid_count,
                records_failed=invalid_count,
                failure_rate=invalid_count / total_count if total_count > 0 else 0,
                message=message,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="consistency_is_valid",
                check_type=CheckType.CONSISTENCY,
                status=CheckStatus.ERROR,
                message=f"Error checking is_valid consistency: {str(e)}"
            )


class GoldDQChecks(BaseDQChecks):
    """
    Gold Layer Data Quality Checks.
    
    Checks:
    - Upstream: Verify Bronze and Silver gates passed
    - Validation: total_events >= unique_users
    - Validation: bot_percentage in range 0-100
    - Validation: risk_score in range 0-100
    - Completeness: Required aggregation fields
    """
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark, "gold", "gold.hourly_stats")
    
    def run_all_checks(
        self, 
        df: DataFrame, 
        run_id: str,
        bronze_passed: bool = True,
        silver_passed: bool = True
    ) -> DQGateResult:
        """Run all Gold layer DQ checks."""
        results = []
        
        # 1. Upstream gate check
        upstream_result = self._check_upstream_gates(bronze_passed, silver_passed)
        results.append(upstream_result)
        
        # If upstream failed, skip other checks
        if not upstream_result.is_passed():
            return DQGateResult(
                layer=self.layer,
                table_name=self.table_name,
                run_id=run_id,
                run_timestamp=datetime.utcnow(),
                checks=results
            )
        
        # 2. Validation: total_events >= unique_users (for hourly_stats)
        events_users_result = self._check_events_users_consistency(df)
        results.append(events_users_result)
        
        # 3. Validation: bot_percentage range
        bot_pct_result = self._check_range(df, "bot_percentage", min_val=0, max_val=100)
        results.append(bot_pct_result)
        
        # 4. Completeness: domain
        domain_result = self._check_completeness(df, "domain", threshold=1.0, is_critical=True)
        results.append(domain_result)
        
        # 5. Completeness: total_events
        events_result = self._check_completeness(df, "total_events", threshold=1.0, is_critical=True)
        results.append(events_result)
        
        return DQGateResult(
            layer=self.layer,
            table_name=self.table_name,
            run_id=run_id,
            run_timestamp=datetime.utcnow(),
            checks=results
        )
    
    def run_risk_score_checks(
        self,
        df: DataFrame,
        run_id: str,
        bronze_passed: bool = True,
        silver_passed: bool = True
    ) -> DQGateResult:
        """Run DQ checks on risk_scores table."""
        results = []
        
        # 1. Upstream gate check
        upstream_result = self._check_upstream_gates(bronze_passed, silver_passed)
        results.append(upstream_result)
        
        if not upstream_result.is_passed():
            return DQGateResult(
                layer=self.layer,
                table_name="gold.risk_scores",
                run_id=run_id,
                run_timestamp=datetime.utcnow(),
                checks=results
            )
        
        # 2. Validation: risk_score range 0-100
        risk_score_result = self._check_range(df, "risk_score", min_val=0, max_val=100)
        results.append(risk_score_result)
        
        # 3. Validation: risk_level in allowed set
        risk_level_result = self._check_value_in_set(
            df, "risk_level", ["LOW", "MEDIUM", "HIGH"]
        )
        results.append(risk_level_result)
        
        # 4. Completeness: entity_id
        entity_result = self._check_completeness(df, "entity_id", threshold=1.0, is_critical=True)
        results.append(entity_result)
        
        return DQGateResult(
            layer=self.layer,
            table_name="gold.risk_scores",
            run_id=run_id,
            run_timestamp=datetime.utcnow(),
            checks=results
        )
    
    def _check_upstream_gates(
        self, 
        bronze_passed: bool, 
        silver_passed: bool
    ) -> DQCheckResult:
        """Check that upstream gates (Bronze and Silver) passed."""
        start_time = datetime.utcnow()
        
        evidence = {
            "bronze_gate_passed": bronze_passed,
            "silver_gate_passed": silver_passed
        }
        
        duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        if bronze_passed and silver_passed:
            return DQCheckResult(
                check_name="upstream_gates_passed",
                check_type=CheckType.UPSTREAM,
                status=CheckStatus.PASSED,
                metric_value=1.0,
                threshold_value=1.0,
                message="All upstream DQ gates passed",
                evidence=evidence,
                check_duration_ms=duration_ms
            )
        else:
            failed_gates = []
            if not bronze_passed:
                failed_gates.append("Bronze")
            if not silver_passed:
                failed_gates.append("Silver")
            
            return DQCheckResult(
                check_name="upstream_gates_passed",
                check_type=CheckType.UPSTREAM,
                status=CheckStatus.FAILED,
                metric_value=0.0,
                threshold_value=1.0,
                message=f"Upstream gates failed: {', '.join(failed_gates)}",
                evidence=evidence,
                check_duration_ms=duration_ms
            )
    
    def _check_events_users_consistency(self, df: DataFrame) -> DQCheckResult:
        """
        Check that total_events >= unique_users (logical consistency).
        You can't have more users than events.
        """
        start_time = datetime.utcnow()
        
        try:
            # Filter records with both values
            check_df = df.filter(
                col("total_events").isNotNull() & 
                col("unique_users").isNotNull()
            )
            total_count = check_df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name="consistency_events_vs_users",
                    check_type=CheckType.CONSISTENCY,
                    status=CheckStatus.SKIPPED,
                    message="No records with both metrics"
                )
            
            # Count violations
            violation_count = check_df.filter(
                col("total_events") < col("unique_users")
            ).count()
            
            valid_count = total_count - violation_count
            consistency_rate = valid_count / total_count
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            if violation_count == 0:
                status = CheckStatus.PASSED
                message = "All records: total_events >= unique_users"
            else:
                status = CheckStatus.FAILED
                message = f"Found {violation_count} records where total_events < unique_users"
            
            return DQCheckResult(
                check_name="consistency_events_vs_users",
                check_type=CheckType.CONSISTENCY,
                status=status,
                metric_value=consistency_rate,
                threshold_value=1.0,
                records_checked=total_count,
                records_passed=valid_count,
                records_failed=violation_count,
                failure_rate=violation_count / total_count if total_count > 0 else 0,
                message=message,
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="consistency_events_vs_users",
                check_type=CheckType.CONSISTENCY,
                status=CheckStatus.ERROR,
                message=f"Error checking consistency: {str(e)}"
            )

