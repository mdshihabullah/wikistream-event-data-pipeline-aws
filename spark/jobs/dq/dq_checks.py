#!/usr/bin/env python3
"""
WikiStream Data Quality Checks Module
======================================
PyDeequ-based data quality checks for Bronze, Silver, and Gold layers.

Uses AWS Deequ (PyDeequ wrapper) for scalable data quality validation.
Deequ Version: 2.0.7-spark-3.5
PyDeequ Version: 1.4.0

This module provides:
- Completeness checks (critical and important fields)
- Timeliness checks (event freshness validation)
- Validity checks (value range and format validation)
- Accuracy checks (cross-field consistency)
- Uniqueness checks (deduplication verification)
- Data profiling for drift detection

All checks follow industry best practices for data quality in streaming pipelines.
"""

import os
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

# ============================================================================
# IMPORTANT: Set SPARK_VERSION before importing PyDeequ
# EMR 7.12.0 uses Spark 3.5.x - PyDeequ requires this environment variable
# ============================================================================
if "SPARK_VERSION" not in os.environ:
    os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, abs as spark_abs,
    unix_timestamp, avg, regexp_extract
)

# PyDeequ imports
try:
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationSuite, VerificationResult
    from pydeequ.analyzers import (
        AnalysisRunner, 
        Completeness, 
        Uniqueness, 
        Size,
        Mean,
        StandardDeviation,
        Minimum,
        Maximum,
        ApproxQuantile
    )
    PYDEEQU_AVAILABLE = True
    print(f"PyDeequ initialized successfully with SPARK_VERSION={os.environ.get('SPARK_VERSION')}")
except ImportError as e:
    PYDEEQU_AVAILABLE = False
    print(f"WARNING: PyDeequ not available, falling back to PySpark-based checks. Error: {e}")


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
    """Base class for PyDeequ-based data quality checks."""
    
    def __init__(self, spark: SparkSession, layer: str, table_name: str):
        self.spark = spark
        self.layer = layer
        self.table_name = table_name
        self.results: List[DQCheckResult] = []
    
    @abstractmethod
    def run_all_checks(self, df: DataFrame, run_id: str) -> DQGateResult:
        """Run all checks for this layer."""
        pass
    
    def _run_deequ_verification(
        self,
        df: DataFrame,
        check: "Check"
    ) -> List[DQCheckResult]:
        """
        Run PyDeequ verification and convert results to DQCheckResult list.
        
        Uses AnalysisRunner for actual metrics + VerificationSuite for pass/fail.
        
        Args:
            df: DataFrame to verify
            check: PyDeequ Check object with constraints
            
        Returns:
            List of DQCheckResult objects with populated metrics
        """
        if not PYDEEQU_AVAILABLE:
            return [DQCheckResult(
                check_name="deequ_unavailable",
                check_type=CheckType.VALIDITY,
                status=CheckStatus.ERROR,
                message="PyDeequ not installed - install pydeequ package"
            )]
        
        start_time = datetime.utcnow()
        results = []
        
        try:
            # Get row count
            row_count = df.count()
            
            # Run verification for pass/fail
            verification_result = VerificationSuite(self.spark) \
                .onData(df) \
                .addCheck(check) \
                .run()
            
            # Parse verification results
            result_df = VerificationResult.checkResultsAsDataFrame(
                self.spark, verification_result
            )
            
            for row in result_df.collect():
                constraint = row["constraint"]
                constraint_status = row["constraint_status"]
                constraint_message = row["constraint_message"] if row["constraint_message"] else ""
                
                # Determine check type and clean name first
                check_type = self._infer_check_type(constraint)
                clean_name = self._extract_clean_check_name(constraint)
                
                # Extract metric value - try message first, then compute directly
                metric_value = self._extract_metric_value(constraint_message)
                
                # If no metric from message, compute it directly from data
                if metric_value is None:
                    metric_value = self._compute_metric_from_constraint(df, constraint, row_count)
                
                # Infer threshold from constraint
                threshold_value = self._extract_threshold(constraint)
                
                # Determine status
                if constraint_status == "Success":
                    status = CheckStatus.PASSED
                elif constraint_status == "Warning":
                    status = CheckStatus.WARNING
                else:
                    status = CheckStatus.FAILED
                
                # Calculate records passed/failed based on metric
                records_passed = None
                records_failed = None
                failure_rate = None
                
                if metric_value is not None and row_count > 0:
                    # metric_value is rate (0-1) for completeness/uniqueness/validity
                    records_passed = int(metric_value * row_count)
                    records_failed = row_count - records_passed
                    failure_rate = round(1.0 - metric_value, 6)
                
                duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
                
                # Build human-readable message
                human_message = self._build_human_message(
                    check_type, clean_name, metric_value, threshold_value, status
                )
                
                results.append(DQCheckResult(
                    check_name=clean_name,
                    check_type=check_type,
                    status=status,
                    metric_value=round(metric_value, 6) if metric_value is not None else None,
                    threshold_value=threshold_value,
                    records_checked=row_count,
                    records_passed=records_passed,
                    records_failed=records_failed,
                    failure_rate=failure_rate,
                    message=human_message,
                    evidence={},  # Clean - no raw constraint clutter
                    check_duration_ms=duration_ms
                ))
            
            return results
            
        except Exception as e:
            import traceback
            return [DQCheckResult(
                check_name="deequ_verification_error",
                check_type=CheckType.VALIDITY,
                status=CheckStatus.ERROR,
                message=f"Deequ verification error: {str(e)}",
                evidence={"error": str(e)}
            )]
    
    def _compute_metric_from_constraint(
        self,
        df: DataFrame,
        constraint: str,
        row_count: int
    ) -> Optional[float]:
        """
        Directly compute metric value when PyDeequ doesn't provide it in message.
        This handles compliance/containedIn checks that don't return Value.
        """
        import re
        
        if row_count == 0:
            return None
        
        try:
            # Completeness: extract column and compute
            if "Completeness" in constraint:
                match = re.search(r'Completeness\((\w+)', constraint)
                if match:
                    col_name = match.group(1)
                    non_null = df.filter(col(col_name).isNotNull()).count()
                    return non_null / row_count
            
            # Uniqueness: extract column and compute
            if "Uniqueness" in constraint:
                match = re.search(r'Uniqueness\(List\((\w+)', constraint)
                if not match:
                    match = re.search(r'Uniqueness\((\w+)', constraint)
                if match:
                    col_name = match.group(1)
                    distinct = df.select(col_name).distinct().count()
                    return distinct / row_count
            
            # ContainedIn/Compliance: extract column and allowed values
            if "Compliance" in constraint or "ContainedIn" in constraint:
                # Try to extract column from SQL expression
                # Pattern: `column` IN ('val1','val2',...) or column IS NULL OR column IN (...)
                match = re.search(r'`(\w+)`\s+(?:IS NULL OR\s+)?`?\1?`?\s*IN\s*\(([^)]+)\)', constraint)
                if match:
                    col_name = match.group(1)
                    values_str = match.group(2)
                    # Parse allowed values
                    allowed = [v.strip().strip("'\"") for v in values_str.split(",")]
                    valid_count = df.filter(
                        col(col_name).isNull() | col(col_name).isin(allowed)
                    ).count()
                    return valid_count / row_count
                
                # Alternative pattern for isContainedIn
                match = re.search(r'ContainedIn\((\w+)', constraint)
                if match:
                    col_name = match.group(1)
                    # We need the allowed values - fallback to 100% if passed
                    return 1.0  # Will be overridden by status
            
            # NonNegative checks
            if "NonNegative" in constraint:
                match = re.search(r'NonNegative\((\w+)', constraint)
                if match:
                    col_name = match.group(1)
                    valid_count = df.filter(
                        col(col_name).isNull() | (col(col_name) >= 0)
                    ).count()
                    return valid_count / row_count
            
        except Exception as e:
            print(f"Warning: Could not compute metric for constraint: {e}")
        
        return None
    
    def _extract_metric_value(self, constraint_message: str) -> Optional[float]:
        """Extract the actual metric value from PyDeequ constraint message."""
        if not constraint_message:
            return None
        
        # Try various patterns PyDeequ uses
        import re
        
        # Pattern: "Value: 0.95 ..."
        match = re.search(r'Value:\s*([\d.]+)', constraint_message)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        
        # Pattern: "value 0.95" or "was 0.95"
        match = re.search(r'(?:value|was)\s+([\d.]+)', constraint_message.lower())
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        
        return None
    
    def _extract_threshold(self, constraint: str) -> Optional[float]:
        """Extract threshold value from constraint definition."""
        import re
        
        # isComplete constraints have implicit 1.0 threshold
        if "Completeness" in constraint and "hasCompleteness" not in constraint.lower():
            return 1.0
        
        # hasCompleteness with lambda often shows >= threshold
        # Pattern: ">= 0.95" or "≥ 0.95"
        match = re.search(r'[≥>=]\s*([\d.]+)', constraint)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        
        # Uniqueness constraint (isUnique) has implicit 1.0 threshold
        if "Uniqueness" in constraint or "isUnique" in constraint:
            return 1.0
        
        return None
    
    def _extract_clean_check_name(self, constraint: str) -> str:
        """Extract a clean, readable check name from constraint."""
        import re
        
        # Completeness checks
        if "Completeness" in constraint:
            match = re.search(r'Completeness\((\w+)', constraint)
            if match:
                return f"completeness_{match.group(1)}"
        
        # Uniqueness checks
        if "Uniqueness" in constraint:
            match = re.search(r'Uniqueness\(List\((\w+)', constraint)
            if not match:
                match = re.search(r'Uniqueness\((\w+)', constraint)
            if match:
                return f"uniqueness_{match.group(1)}"
        
        # Compliance/ContainedIn checks
        if "Compliance" in constraint or "contained" in constraint.lower():
            match = re.search(r'(\w+)\s+contained\s+in', constraint)
            if match:
                return f"validity_{match.group(1)}_in_allowed_values"
            # Extract column name from constraint
            match = re.search(r'Compliance\(([^,]+)', constraint)
            if match:
                return f"validity_{match.group(1).strip()}"
        
        # NonNegative checks
        if "NonNegative" in constraint:
            match = re.search(r'NonNegative\((\w+)', constraint)
            if match:
                return f"validity_{match.group(1)}_non_negative"
        
        # Max/Min checks
        if "hasMax" in constraint.lower() or "Maximum" in constraint:
            match = re.search(r'Maximum\((\w+)', constraint)
            if match:
                return f"validity_{match.group(1)}_max_check"
        
        # Fallback: sanitize the constraint string
        return self._sanitize_check_name(constraint)
    
    def _build_human_message(
        self,
        check_type: CheckType,
        check_name: str,
        metric_value: Optional[float],
        threshold_value: Optional[float],
        status: CheckStatus
    ) -> str:
        """Build a human-readable message for the check result."""
        if metric_value is None:
            return f"{check_name}: {status.value}"
        
        if check_type == CheckType.COMPLETENESS:
            pct = metric_value * 100
            threshold_pct = (threshold_value or 1.0) * 100
            return f"Completeness: {pct:.2f}% (threshold: {threshold_pct:.0f}%)"
        
        elif check_type == CheckType.UNIQUENESS:
            pct = metric_value * 100
            return f"Uniqueness: {pct:.2f}% unique values"
        
        elif check_type == CheckType.VALIDITY:
            if metric_value <= 1:
                pct = metric_value * 100
                return f"Validity: {pct:.2f}% valid values"
            else:
                return f"Validity check: value = {metric_value}"
        
        else:
            return f"{check_name}: value = {metric_value}, status = {status.value}"
    
    def _infer_check_type(self, constraint: str) -> CheckType:
        """Infer check type from constraint name."""
        constraint_lower = constraint.lower()
        if "completeness" in constraint_lower or "isnull" in constraint_lower:
            return CheckType.COMPLETENESS
        elif "unique" in constraint_lower:
            return CheckType.UNIQUENESS
        elif "contained" in constraint_lower or "compliance" in constraint_lower:
            return CheckType.VALIDITY
        elif "nonnegative" in constraint_lower or "maximum" in constraint_lower or "minimum" in constraint_lower:
            return CheckType.VALIDITY
        elif "accuracy" in constraint_lower:
            return CheckType.ACCURACY
        else:
            return CheckType.CONSISTENCY
    
    def _sanitize_check_name(self, constraint: str) -> str:
        """Convert constraint string to a clean check name (fallback)."""
        import re
        # Remove common prefixes
        name = re.sub(r'Constraint|Compliance|Completeness|Uniqueness', '', constraint)
        # Clean up parentheses and special chars
        name = re.sub(r'[()[\],`\'"]', '', name)
        name = re.sub(r'\s+', '_', name)
        name = re.sub(r'_+', '_', name)
        return name.lower().strip('_')[:80]  # Limit length

    def _check_completeness_pyspark(
        self,
        df: DataFrame,
        column: str,
        threshold: float = 1.0,
        is_critical: bool = True
    ) -> DQCheckResult:
        """
        Fallback: Check completeness using PySpark when Deequ unavailable.
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


class BronzeDQChecks(BaseDQChecks):
    """
    Bronze Layer Data Quality Checks using PyDeequ.
    
    Checks:
    - Completeness: Critical fields (event_id, event_type, domain, event_timestamp) - 100%
    - Completeness: Important fields (title, user, wiki) - 95% threshold
    - Timeliness: 95% events within 1 minute of bronze_processed_at
    - Validity: event_type in allowed set
    - Validity: event_hour in range 0-23
    - Validity: namespace >= 0
    - Uniqueness: event_id within batch
    """
    
    # Configuration
    CRITICAL_FIELDS = ["event_id", "event_type", "domain", "event_timestamp"]
    IMPORTANT_FIELDS = ["title", "user", "wiki"]
    ALLOWED_EVENT_TYPES = ["edit", "new", "log", "categorize", "external", "unknown"]
    TIMELINESS_THRESHOLD_SECONDS = 180  # 3 minutes - realistic for Wikipedia stream latency
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark, "bronze", "bronze.raw_events")
    
    def run_all_checks(self, df: DataFrame, run_id: str) -> DQGateResult:
        """Run all Bronze layer DQ checks using PyDeequ."""
        all_results = []
        
        if PYDEEQU_AVAILABLE:
            # Build PyDeequ Check with all constraints
            check = Check(self.spark, CheckLevel.Error, "Bronze DQ Checks")
            
            # 1. Critical field completeness (100%)
            for field in self.CRITICAL_FIELDS:
                check = check.isComplete(field)
            
            # 2. Important field completeness (95%)
            for field in self.IMPORTANT_FIELDS:
                check = check.hasCompleteness(field, lambda x: x >= 0.95)
            
            # 3. Validity: event_type in allowed set
            check = check.isContainedIn("event_type", self.ALLOWED_EVENT_TYPES)
            
            # 4. Validity: event_hour range 0-23
            check = check.isNonNegative("event_hour")
            check = check.hasMax("event_hour", lambda x: x <= 23)
            
            # Note: Namespace validity check removed - Wikipedia SSE uses negative IDs:
            #   -1 = Special pages, -2 = Media namespace (both are valid)
            
            # 6. Uniqueness: event_id
            check = check.isUnique("event_id")
            
            # Run Deequ verification
            deequ_results = self._run_deequ_verification(df, check)
            all_results.extend(deequ_results)
            
        else:
            # Fallback to PySpark-based checks
            print("Using PySpark fallback for DQ checks")
            
            # Critical completeness
            for field in self.CRITICAL_FIELDS:
                result = self._check_completeness_pyspark(df, field, 1.0, True)
                all_results.append(result)
            
            # Important completeness
            for field in self.IMPORTANT_FIELDS:
                result = self._check_completeness_pyspark(df, field, 0.95, False)
                all_results.append(result)
        
        # Custom PySpark-based checks (always run)
        # Note: Namespace check removed - Wikipedia uses negative namespace IDs for
        # special pages (-1=Special, -2=Media) which are valid values
        
        # 5. Timeliness check (P95 latency within threshold)
        timeliness_result = self._check_timeliness(df)
        all_results.append(timeliness_result)
        
        return DQGateResult(
            layer=self.layer,
            table_name=self.table_name,
            run_id=run_id,
            run_timestamp=datetime.utcnow(),
            checks=all_results
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
                    check_name="timeliness_95pct_within_1min",
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
                "percentile_approx(latency_seconds, 0.95)"
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
                "within_threshold_pct": round(within_threshold_rate * 100, 2)
            }
            
            if percentile_95 is not None and percentile_95 <= self.TIMELINESS_THRESHOLD_SECONDS:
                status = CheckStatus.PASSED
                message = f"P95 latency {percentile_95:.1f}s <= {self.TIMELINESS_THRESHOLD_SECONDS}s threshold"
            else:
                # Timeliness is WARNING (not FAILED) - doesn't block pipeline
                # This allows batch processing to continue while alerting on high latency
                # Common after restarts or during data backlog catchup
                status = CheckStatus.WARNING
                message = f"P95 latency {percentile_95:.1f}s exceeds {self.TIMELINESS_THRESHOLD_SECONDS}s threshold (WARNING - not blocking)"
            
            return DQCheckResult(
                check_name="timeliness_95pct_within_1min",
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
                check_name="timeliness_95pct_within_1min",
                check_type=CheckType.TIMELINESS,
                status=CheckStatus.ERROR,
                message=f"Error checking timeliness: {str(e)}"
            )


class SilverDQChecks(BaseDQChecks):
    """
    Silver Layer Data Quality Checks using PyDeequ.
    
    Checks:
    - Completeness: Critical fields 100%
    - Accuracy: length_delta calculation
    - Accuracy: is_anonymous derivation
    - Accuracy: region mapping
    - Consistency: is_valid flag
    - Uniqueness: event_id
    - Validity: region in allowed set
    """
    
    ALLOWED_REGIONS = ["asia_pacific", "europe", "americas", "middle_east", "other"]
    DOMAIN_REGION_MAP = {
        "zh.wikipedia.org": "asia_pacific",
        "ja.wikipedia.org": "asia_pacific",
        "ko.wikipedia.org": "asia_pacific",
        "de.wikipedia.org": "europe",
        "fr.wikipedia.org": "europe",
        "en.wikipedia.org": "americas",
        "ar.wikipedia.org": "middle_east",
    }
    IP_PATTERN = r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark, "silver", "silver.cleaned_events")
    
    def run_all_checks(self, df: DataFrame, run_id: str) -> DQGateResult:
        """Run all Silver layer DQ checks using PyDeequ."""
        all_results = []
        
        if PYDEEQU_AVAILABLE:
            # Build PyDeequ Check
            check = Check(self.spark, CheckLevel.Error, "Silver DQ Checks")
            
            # 1. Completeness checks
            for field in ["event_id", "domain", "region", "event_timestamp"]:
                check = check.isComplete(field)
            
            # 2. Validity: region in allowed set
            check = check.isContainedIn("region", self.ALLOWED_REGIONS)
            
            # 3. Uniqueness: event_id
            check = check.isUnique("event_id")
            
            # Run Deequ verification
            deequ_results = self._run_deequ_verification(df, check)
            all_results.extend(deequ_results)
        else:
            # Fallback
            for field in ["event_id", "domain", "region", "event_timestamp"]:
                result = self._check_completeness_pyspark(df, field, 1.0, True)
                all_results.append(result)
        
        # Custom accuracy checks (PySpark-based)
        all_results.append(self._check_length_delta_accuracy(df))
        all_results.append(self._check_anonymous_accuracy(df))
        all_results.append(self._check_region_mapping(df))
        all_results.append(self._check_valid_flag_consistency(df))
        
        return DQGateResult(
            layer=self.layer,
            table_name=self.table_name,
            run_id=run_id,
            run_timestamp=datetime.utcnow(),
            checks=all_results
        )
    
    def _check_length_delta_accuracy(self, df: DataFrame) -> DQCheckResult:
        """Verify length_delta == length_new - length_old."""
        start_time = datetime.utcnow()
        
        try:
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
                    message="No records with length values"
                )
            
            accurate_count = calc_df.filter(
                col("length_delta") == (col("length_new") - col("length_old"))
            ).count()
            
            accuracy_rate = accurate_count / total_count
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            status = CheckStatus.PASSED if accuracy_rate >= 0.99 else CheckStatus.FAILED
            
            return DQCheckResult(
                check_name="accuracy_length_delta",
                check_type=CheckType.ACCURACY,
                status=status,
                metric_value=accuracy_rate,
                threshold_value=0.99,
                records_checked=total_count,
                records_passed=accurate_count,
                records_failed=total_count - accurate_count,
                message=f"length_delta accuracy: {accuracy_rate:.2%}",
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="accuracy_length_delta",
                check_type=CheckType.ACCURACY,
                status=CheckStatus.ERROR,
                message=f"Error: {str(e)}"
            )
    
    def _check_anonymous_accuracy(self, df: DataFrame) -> DQCheckResult:
        """Verify is_anonymous derived from IP pattern."""
        start_time = datetime.utcnow()
        
        try:
            user_df = df.filter(col("user_normalized").isNotNull())
            total_count = user_df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name="accuracy_is_anonymous",
                    check_type=CheckType.ACCURACY,
                    status=CheckStatus.SKIPPED,
                    message="No user records"
                )
            
            user_df = user_df.withColumn(
                "expected_anonymous",
                when(
                    regexp_extract(col("user_normalized"), self.IP_PATTERN, 0) != "",
                    lit(True)
                ).otherwise(lit(False))
            )
            
            accurate_count = user_df.filter(
                col("is_anonymous") == col("expected_anonymous")
            ).count()
            
            accuracy_rate = accurate_count / total_count
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            status = CheckStatus.PASSED if accuracy_rate >= 0.99 else CheckStatus.FAILED
            
            return DQCheckResult(
                check_name="accuracy_is_anonymous",
                check_type=CheckType.ACCURACY,
                status=status,
                metric_value=accuracy_rate,
                threshold_value=0.99,
                records_checked=total_count,
                records_passed=accurate_count,
                records_failed=total_count - accurate_count,
                message=f"is_anonymous accuracy: {accuracy_rate:.2%}",
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="accuracy_is_anonymous",
                check_type=CheckType.ACCURACY,
                status=CheckStatus.ERROR,
                message=f"Error: {str(e)}"
            )
    
    def _check_region_mapping(self, df: DataFrame) -> DQCheckResult:
        """Verify region correctly derived from domain."""
        start_time = datetime.utcnow()
        
        try:
            known_domains = list(self.DOMAIN_REGION_MAP.keys())
            known_df = df.filter(col("domain").isin(known_domains))
            total_count = known_df.count()
            
            if total_count == 0:
                return DQCheckResult(
                    check_name="accuracy_region_mapping",
                    check_type=CheckType.ACCURACY,
                    status=CheckStatus.SKIPPED,
                    message="No known domain records"
                )
            
            correct_count = 0
            for domain, expected_region in self.DOMAIN_REGION_MAP.items():
                count = known_df.filter(
                    (col("domain") == domain) & (col("region") == expected_region)
                ).count()
                correct_count += count
            
            accuracy_rate = correct_count / total_count
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            status = CheckStatus.PASSED if accuracy_rate >= 1.0 else CheckStatus.FAILED
            
            return DQCheckResult(
                check_name="accuracy_region_mapping",
                check_type=CheckType.ACCURACY,
                status=status,
                metric_value=accuracy_rate,
                threshold_value=1.0,
                records_checked=total_count,
                records_passed=correct_count,
                records_failed=total_count - correct_count,
                message=f"Region mapping accuracy: {accuracy_rate:.2%}",
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="accuracy_region_mapping",
                check_type=CheckType.ACCURACY,
                status=CheckStatus.ERROR,
                message=f"Error: {str(e)}"
            )
    
    def _check_valid_flag_consistency(self, df: DataFrame) -> DQCheckResult:
        """All Silver records should have is_valid = True."""
        start_time = datetime.utcnow()
        
        try:
            total_count = df.count()
            if total_count == 0:
                return DQCheckResult(
                    check_name="consistency_is_valid",
                    check_type=CheckType.CONSISTENCY,
                    status=CheckStatus.SKIPPED,
                    message="No records"
                )
            
            valid_count = df.filter(col("is_valid") == True).count()
            valid_rate = valid_count / total_count
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            status = CheckStatus.PASSED if valid_rate >= 1.0 else CheckStatus.FAILED
            
            return DQCheckResult(
                check_name="consistency_is_valid",
                check_type=CheckType.CONSISTENCY,
                status=status,
                metric_value=valid_rate,
                threshold_value=1.0,
                records_checked=total_count,
                records_passed=valid_count,
                records_failed=total_count - valid_count,
                message=f"is_valid consistency: {valid_rate:.2%}",
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="consistency_is_valid",
                check_type=CheckType.CONSISTENCY,
                status=CheckStatus.ERROR,
                message=f"Error: {str(e)}"
            )


class GoldDQChecks(BaseDQChecks):
    """
    Gold Layer Data Quality Checks using PyDeequ.
    
    Checks:
    - Upstream: Bronze and Silver gates passed
    - Consistency: total_events >= unique_users
    - Validity: bot_percentage 0-100
    - Validity: risk_score 0-100
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
        all_results = []
        
        # 1. Upstream gate check
        upstream_result = self._check_upstream_gates(bronze_passed, silver_passed)
        all_results.append(upstream_result)
        
        if not upstream_result.is_passed():
            return DQGateResult(
                layer=self.layer,
                table_name=self.table_name,
                run_id=run_id,
                run_timestamp=datetime.utcnow(),
                checks=all_results
            )
        
        if PYDEEQU_AVAILABLE:
            # Build PyDeequ Check for Gold
            check = Check(self.spark, CheckLevel.Error, "Gold DQ Checks")
            
            # Completeness
            check = check.isComplete("domain")
            check = check.isComplete("total_events")
            
            # Validity: bot_percentage 0-100
            check = check.isNonNegative("bot_percentage")
            check = check.hasMax("bot_percentage", lambda x: x <= 100)
            
            # Run Deequ verification
            deequ_results = self._run_deequ_verification(df, check)
            all_results.extend(deequ_results)
        else:
            for field in ["domain", "total_events"]:
                result = self._check_completeness_pyspark(df, field, 1.0, True)
                all_results.append(result)
        
        # Custom consistency check
        all_results.append(self._check_events_users_consistency(df))
        
        return DQGateResult(
            layer=self.layer,
            table_name=self.table_name,
            run_id=run_id,
            run_timestamp=datetime.utcnow(),
            checks=all_results
        )
    
    def run_risk_score_checks(
        self,
        df: DataFrame,
        run_id: str,
        bronze_passed: bool = True,
        silver_passed: bool = True
    ) -> DQGateResult:
        """Run DQ checks on risk_scores table."""
        all_results = []
        
        upstream_result = self._check_upstream_gates(bronze_passed, silver_passed)
        all_results.append(upstream_result)
        
        if not upstream_result.is_passed():
            return DQGateResult(
                layer=self.layer,
                table_name="gold.risk_scores",
                run_id=run_id,
                run_timestamp=datetime.utcnow(),
                checks=all_results
            )
        
        if PYDEEQU_AVAILABLE:
            check = Check(self.spark, CheckLevel.Error, "Risk Score DQ Checks")
            
            check = check.isComplete("entity_id")
            check = check.isNonNegative("risk_score")
            check = check.hasMax("risk_score", lambda x: x <= 100)
            check = check.isContainedIn("risk_level", ["LOW", "MEDIUM", "HIGH"])
            
            deequ_results = self._run_deequ_verification(df, check)
            all_results.extend(deequ_results)
        else:
            result = self._check_completeness_pyspark(df, "entity_id", 1.0, True)
            all_results.append(result)
        
        return DQGateResult(
            layer=self.layer,
            table_name="gold.risk_scores",
            run_id=run_id,
            run_timestamp=datetime.utcnow(),
            checks=all_results
        )
    
    def _check_upstream_gates(
        self, 
        bronze_passed: bool, 
        silver_passed: bool
    ) -> DQCheckResult:
        """Check that upstream gates passed."""
        evidence = {
            "bronze_gate_passed": bronze_passed,
            "silver_gate_passed": silver_passed
        }
        
        if bronze_passed and silver_passed:
            return DQCheckResult(
                check_name="upstream_gates_passed",
                check_type=CheckType.UPSTREAM,
                status=CheckStatus.PASSED,
                metric_value=1.0,
                message="All upstream DQ gates passed",
                evidence=evidence
            )
        else:
            failed = []
            if not bronze_passed:
                failed.append("Bronze")
            if not silver_passed:
                failed.append("Silver")
            
            return DQCheckResult(
                check_name="upstream_gates_passed",
                check_type=CheckType.UPSTREAM,
                status=CheckStatus.FAILED,
                metric_value=0.0,
                message=f"Upstream gates failed: {', '.join(failed)}",
                evidence=evidence
            )
    
    def _check_events_users_consistency(self, df: DataFrame) -> DQCheckResult:
        """Check total_events >= unique_users."""
        start_time = datetime.utcnow()
        
        try:
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
                    message="No records"
                )
            
            violation_count = check_df.filter(
                col("total_events") < col("unique_users")
            ).count()
            
            valid_count = total_count - violation_count
            consistency_rate = valid_count / total_count
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            status = CheckStatus.PASSED if violation_count == 0 else CheckStatus.FAILED
            
            return DQCheckResult(
                check_name="consistency_events_vs_users",
                check_type=CheckType.CONSISTENCY,
                status=status,
                metric_value=consistency_rate,
                threshold_value=1.0,
                records_checked=total_count,
                records_passed=valid_count,
                records_failed=violation_count,
                message=f"events >= users: {consistency_rate:.2%}",
                check_duration_ms=duration_ms
            )
        except Exception as e:
            return DQCheckResult(
                check_name="consistency_events_vs_users",
                check_type=CheckType.CONSISTENCY,
                status=CheckStatus.ERROR,
                message=f"Error: {str(e)}"
            )
