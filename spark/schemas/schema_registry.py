#!/usr/bin/env python3
"""
WikiStream Schema Registry Module
===============================
Centralized schema management for all pipeline tables.

Features:
- Load schema contracts from JSON
- Validate dataframes against schemas
- Detect schema drift
- Manage schema evolution
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, BooleanType, TimestampType, DoubleType
)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class SchemaValidationStatus(Enum):
    """Status of schema validation."""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"


class CompatibilityType(Enum):
    """Type of schema change."""
    BACKWARD_COMPATIBLE = "backward_compatible"
    BREAKING = "breaking"


# =============================================================================
# Schema Contract Loader
# =============================================================================

class SchemaContractLoader:
    """Loads and parses schema contract JSON files."""
    
    def __init__(self, schemas_dir: str = None):
        """
        Initialize schema contract loader.
        
        Args:
            schemas_dir: Directory containing schema contract JSON files.
                        Defaults to spark/schemas/ directory.
        """
        if schemas_dir is None:
            # Default to package directory
            self.schemas_dir = Path(__file__).parent
        else:
            self.schemas_dir = Path(schemas_dir)
        
        self._cache: Dict[str, Dict] = {}
    
    def load_contract(self, contract_file: str = "schema_v1_contract.json") -> Dict:
        """
        Load schema contract from JSON file.
        
        Args:
            contract_file: Name of contract JSON file
            
        Returns:
            Parsed contract dictionary
        """
        contract_path = self.schemas_dir / contract_file
        
        if contract_path not in self._cache:
            logger.info(f"Loading schema contract from {contract_path}")
            with open(contract_path, 'r') as f:
                contract = json.load(f)
            self._cache[contract_path] = contract_path
        
        return self._cache[contract_path]
    
    def get_table_schema(self, table_name: str, contract_file: str = "schema_v1_contract.json") -> Dict:
        """
        Get schema definition for a specific table.
        
        Args:
            table_name: Full table name (e.g., "bronze.raw_events")
            contract_file: Contract file name
            
        Returns:
            Table schema dictionary
        """
        contract = self.load_contract(contract_file)
        
        if "tables" not in contract:
            raise ValueError(f"Contract missing 'tables' section: {contract_file}")
        
        if table_name not in contract["tables"]:
            raise ValueError(f"Table '{table_name}' not found in contract")
        
        return contract["tables"][table_name]
    
    def get_field_definition(self, table_name: str, field_name: str) -> Dict:
        """
        Get definition for a specific field.
        
        Args:
            table_name: Full table name
            field_name: Field name
            
        Returns:
            Field definition dictionary
        """
        table_schema = self.get_table_schema(table_name)
        
        if "fields" not in table_schema:
            raise ValueError(f"Table '{table_name}' missing 'fields' section")
        
        if field_name not in table_schema["fields"]:
            raise ValueError(f"Field '{field_name}' not found in table '{table_name}'")
        
        return table_schema["fields"][field_name]
    
    def get_evolution_policy(self, contract_file: str = "schema_v1_contract.json") -> Dict:
        """Get schema evolution policy."""
        contract = self.load_contract(contract_file)
        return contract.get("evolution_policy", {})
    
    def get_compatibility_matrix(self, contract_file: str = "schema_v1_contract.json") -> Dict:
        """Get version compatibility matrix."""
        contract = self.load_contract(contract_file)
        return contract.get("compatibility_matrix", {})


# =============================================================================
# Schema Validator
# =============================================================================

class SchemaValidator:
    """Validates DataFrames against schema contracts."""
    
    def __init__(self, spark: SparkSession, schema_loader: SchemaContractLoader):
        """
        Initialize schema validator.
        
        Args:
            spark: SparkSession for dataframe operations
            schema_loader: SchemaContractLoader instance
        """
        self.spark = spark
        self.schema_loader = schema_loader
    
    def validate_dataframe(
        self,
        df: DataFrame,
        table_name: str,
        contract_file: str = "schema_v1_contract.json"
    ) -> Tuple[SchemaValidationStatus, List[Dict]]:
        """
        Validate DataFrame against schema contract.
        
        Args:
            df: DataFrame to validate
            table_name: Table name (e.g., "bronze.raw_events")
            contract_file: Schema contract file name
            
        Returns:
            Tuple of (status, list of validation issues)
        """
        table_schema = self.schema_loader.get_table_schema(table_name, contract_file)
        issues = []
        
        # Get actual DataFrame schema
        df_schema_dict = {field.name: field for field in df.schema.fields}
        
        # Check required fields
        for field_name, field_def in table_schema["fields"].items():
            expected_type = field_def["type"]
            is_nullable = field_def.get("nullable", False)
            
            # Check if field exists
            if field_name not in df_schema_dict:
                if not is_nullable:
                    issues.append({
                        "field": field_name,
                        "issue": "MISSING_REQUIRED_FIELD",
                        "severity": "CRITICAL",
                        "message": f"Required field '{field_name}' missing from DataFrame",
                        "expected_type": expected_type,
                        "actual_type": None
                    })
                else:
                    issues.append({
                        "field": field_name,
                        "issue": "MISSING_OPTIONAL_FIELD",
                        "severity": "WARNING",
                        "message": f"Optional field '{field_name}' missing from DataFrame",
                        "expected_type": expected_type,
                        "actual_type": None
                    })
                continue
            
            # Check if field null when it shouldn't be
            if not is_nullable:
                null_count = df.filter(col(field_name).isNull()).count()
                if null_count > 0:
                    issues.append({
                        "field": field_name,
                        "issue": "NULL_IN_REQUIRED_FIELD",
                        "severity": "CRITICAL",
                        "message": f"Required field '{field_name}' has {null_count} null values",
                        "null_count": null_count
                    })
        
        # Check for extra fields not in contract
        df_field_names = set(df_schema_dict.keys())
        contract_field_names = set(table_schema["fields"].keys())
        extra_fields = df_field_names - contract_field_names
        
        for field_name in extra_fields:
            issues.append({
                "field": field_name,
                "issue": "UNDECLARED_FIELD",
                "severity": "WARNING",
                "message": f"Field '{field_name}' in DataFrame but not in schema contract",
                "actual_type": str(df_schema_dict[field_name].dataType)
            })
        
        # Determine overall status
        critical_issues = [i for i in issues if i["severity"] == "CRITICAL"]
        status = SchemaValidationStatus.FAILED if critical_issues else SchemaValidationStatus.PASSED
        
        return status, issues
    
    def enforce_schema_constraints(
        self,
        df: DataFrame,
        table_name: str,
        contract_file: str = "schema_v1_contract.json"
    ) -> DataFrame:
        """
        Enforce schema constraints on DataFrame.
        
        This includes:
        - Type casting to correct types
        - Setting fields to NULL for invalid data
        - Applying validation rules (ranges, patterns, allowed values)
        
        Args:
            df: DataFrame to enforce constraints on
            table_name: Table name
            contract_file: Schema contract file name
            
        Returns:
            DataFrame with constraints enforced
        """
        table_schema = self.schema_loader.get_table_schema(table_name, contract_file)
        enforced_df = df
        
        # Log enforcement start
        logger.info(f"Enforcing schema constraints for table: {table_name}")
        
        # Apply validation rules for each field
        for field_name, field_def in table_schema["fields"].items():
            expected_type = field_def["type"]
            validation = field_def.get("validation", {})
            
            # Skip if field not in DataFrame
            if field_name not in enforced_df.columns:
                continue
            
            # Type casting
            enforced_df = self._type_cast_field(enforced_df, field_name, expected_type)
            
            # Apply validation rules
            if "range" in validation:
                enforced_df = self._apply_range_validation(
                    enforced_df, field_name, validation["range"]
                )
            
            if "pattern" in validation:
                enforced_df = self._apply_pattern_validation(
                    enforced_df, field_name, validation["pattern"]
                )
            
            if "allowed_values" in validation:
                enforced_df = self._apply_allowed_values_validation(
                    enforced_df, field_name, validation["allowed_values"]
                )
        
        logger.info(f"Schema constraints enforced for {len(table_schema['fields'])} fields")
        return enforced_df
    
    def _type_cast_field(self, df: DataFrame, field_name: str, expected_type: str) -> DataFrame:
        """Safely cast field to expected type."""
        type_mapping = {
            "STRING": "string",
            "INT": "integer",
            "BIGINT": "long",
            "BOOLEAN": "boolean",
            "TIMESTAMP": "timestamp",
            "DOUBLE": "double"
        }
        
        spark_type = type_mapping.get(expected_type, "string")
        
        try:
            # Attempt type cast
            return df.withColumn(field_name, col(field_name).cast(spark_type))
        except Exception as e:
            logger.warning(f"Failed to cast {field_name} to {expected_type}: {e}")
            # If cast fails, keep original but log warning
            return df
    
    def _apply_range_validation(
        self,
        df: DataFrame,
        field_name: str,
        range_def: Dict[str, int]
    ) -> DataFrame:
        """
        Apply range validation constraint.
        
        Invalid values are set to NULL.
        
        Args:
            df: DataFrame to validate
            field_name: Field name
            range_def: Range definition with 'min' and 'max' keys
            
        Returns:
            DataFrame with invalid values set to NULL
        """
        min_val = range_def.get("min")
        max_val = range_def.get("max")
        
        if min_val is not None and max_val is not None:
            df = df.withColumn(
                field_name,
                when(
                    (col(field_name) >= min_val) & (col(field_name) <= max_val),
                    col(field_name)
                ).otherwise(None)
            )
        
        return df
    
    def _apply_pattern_validation(
        self,
        df: DataFrame,
        field_name: str,
        pattern: str
    ) -> DataFrame:
        """
        Apply regex pattern validation constraint.
        
        Invalid values are set to NULL.
        
        Args:
            df: DataFrame to validate
            field_name: Field name
            pattern: Regex pattern string
            
        Returns:
            DataFrame with invalid values set to NULL
        """
        from pyspark.sql.functions import regexp_extract
        
        # rlike checks if pattern matches
        df = df.withColumn(
            f"{field_name}_valid",
            col(field_name).rlike(pattern)
        )
        
        # Set to NULL if pattern doesn't match
        df = df.withColumn(
            field_name,
            when(col(f"{field_name}_valid"), col(field_name)).otherwise(None)
        )
        
        # Drop temporary column
        df = df.drop(f"{field_name}_valid")
        
        return df
    
    def _apply_allowed_values_validation(
        self,
        df: DataFrame,
        field_name: str,
        allowed_values: List[str]
    ) -> DataFrame:
        """
        Apply allowed values validation constraint.
        
        Invalid values are set to NULL.
        
        Args:
            df: DataFrame to validate
            field_name: Field name
            allowed_values: List of allowed values
            
        Returns:
            DataFrame with invalid values set to NULL
        """
        df = df.withColumn(
            field_name,
            when(col(field_name).isin(allowed_values), col(field_name)).otherwise(None)
        )
        
        return df


# =============================================================================
# Schema Drift Detector
# =============================================================================

class SchemaDriftDetector:
    """Detects schema drift between current and baseline profiles."""
    
    def __init__(self, spark: SparkSession, schema_loader: SchemaContractLoader):
        """
        Initialize schema drift detector.
        
        Args:
            spark: SparkSession for dataframe operations
            schema_loader: SchemaContractLoader instance
        """
        self.spark = spark
        self.schema_loader = schema_loader
    
    def detect_schema_drift(
        self,
        current_df: DataFrame,
        table_name: str,
        baseline_profile: Optional[Dict] = None,
        contract_file: str = "schema_v1_contract.json"
    ) -> List[Dict]:
        """
        Detect schema drift by comparing current schema to contract and baseline.
        
        Args:
            current_df: Current DataFrame to analyze
            table_name: Table name
            baseline_profile: Historical profile (optional)
            contract_file: Schema contract file name
            
        Returns:
            List of drift detections
        """
        table_schema = self.schema_loader.get_table_schema(table_name, contract_file)
        drifts = []
        
        # 1. Check for schema drift (added/removed columns, type changes)
        current_columns = set(current_df.columns)
        contract_columns = set(table_schema["fields"].keys())
        
        added_columns = current_columns - contract_columns
        removed_columns = contract_columns - current_columns
        
        for col_name in added_columns:
            drifts.append({
                "type": "COLUMN_ADDED",
                "field": col_name,
                "severity": "WARNING",
                "message": f"New column '{col_name}' detected in DataFrame but not in schema contract"
            })
        
        for col_name in removed_columns:
            if not table_schema["fields"][col_name].get("nullable", False):
                # Only warn if required field is missing
                drifts.append({
                    "type": "COLUMN_REMOVED",
                    "field": col_name,
                    "severity": "CRITICAL",
                    "message": f"Required field '{col_name}' missing from DataFrame"
                })
        
        # 2. Check for type changes if baseline available
        if baseline_profile:
            for col_name, col_type in baseline_profile.get("column_types", {}).items():
                if col_name in current_columns:
                    current_type = str(current_df.schema[col_name].dataType)
                    if current_type != col_type:
                        drifts.append({
                            "type": "TYPE_CHANGE",
                            "field": col_name,
                            "severity": "CRITICAL",
                            "message": f"Type change detected in '{col_name}': {col_type} -> {current_type}",
                            "old_type": col_type,
                            "new_type": current_type
                        })
        
        return drifts
    
    def check_backward_compatibility(
        self,
        new_schema: Dict,
        old_schema: Dict,
        contract_file: str = "schema_v1_contract.json"
    ) -> Tuple[CompatibilityType, List[str]]:
        """
        Check if new schema is backward compatible with old schema.
        
        Args:
            new_schema: New schema definition
            old_schema: Old schema definition
            contract_file: Schema contract file name
            
        Returns:
            Tuple of (compatibility_type, list of issues)
        """
        evolution_policy = self.schema_loader.get_evolution_policy(contract_file)
        backward_changes = evolution_policy.get("backward_compatible_changes", [])
        breaking_changes = evolution_policy.get("breaking_changes", [])
        
        issues = []
        
        # Check for breaking changes
        new_fields = set(new_schema.get("fields", {}).keys())
        old_fields = set(old_schema.get("fields", {}).keys())
        
        for change_def in breaking_changes:
            change_type = change_def["type"]
            
            if change_type == "remove_required_field":
                removed_required = old_fields - new_fields
                for field in removed_required:
                    if not old_schema["fields"][field].get("nullable", False):
                        issues.append(f"Required field '{field}' removed")
            
            elif change_type == "change_field_type":
                # Check if any field types changed
                for field in new_fields & old_fields:
                    new_type = new_schema["fields"][field]["type"]
                    old_type = old_schema["fields"][field]["type"]
                    if new_type != old_type:
                        issues.append(f"Type changed for '{field}': {old_type} -> {new_type}")
            
            elif change_type == "rename_field":
                # Hard to detect field renames automatically
                # This would require manual verification
                pass
            
            elif change_type == "change_nullable_to_required":
                # Check if any field became required
                for field in new_fields & old_fields:
                    was_nullable = old_schema["fields"][field].get("nullable", True)
                    is_required = not new_schema["fields"][field].get("nullable", False)
                    if was_nullable and is_required:
                        issues.append(f"Field '{field}' changed from nullable to required")
        
        if issues:
            return CompatibilityType.BREAKING, issues
        
        return CompatibilityType.BACKWARD_COMPATIBLE, []


# =============================================================================
# Schema Version Manager
# =============================================================================

class SchemaVersionManager:
    """Manages schema versions and evolution."""
    
    def __init__(self, schema_loader: SchemaContractLoader):
        """
        Initialize schema version manager.
        
        Args:
            schema_loader: SchemaContractLoader instance
        """
        self.schema_loader = schema_loader
    
    def get_current_version(self, contract_file: str = "schema_v1_contract.json") -> str:
        """Get current schema version."""
        contract = self.schema_loader.load_contract(contract_file)
        return contract.get("current_version", "unknown")
    
    def propose_next_version(self, contract_file: str = "schema_v1_contract.json") -> str:
        """
        Propose next schema version based on semantic versioning.
        
        Format: MAJOR.MINOR.PATCH (e.g., 1.0.1 -> 1.0.2)
        """
        current_version = self.get_current_version(contract_file)
        
        try:
            major, minor, patch = map(int, current_version.split("."))
            return f"{major}.{minor}.{patch + 1}"
        except Exception as e:
            logger.error(f"Failed to parse version {current_version}: {e}")
            return f"{current_version}.next"
    
    def validate_version_format(self, version: str) -> bool:
        """
        Validate version follows semantic versioning.
        
        Args:
            version: Version string
            
        Returns:
            True if valid, False otherwise
        """
        import re
        pattern = r"^\d+\.\d+\.\d+$"
        return bool(re.match(pattern, version))


# =============================================================================
# Helper Functions
# =============================================================================

def create_spark_schema_from_contract(
    spark: SparkSession,
    table_name: str,
    contract_file: str = "schema_v1_contract.json"
) -> StructType:
    """
    Create PySpark StructType from schema contract.
    
    Args:
        spark: SparkSession (for type conversion)
        table_name: Table name
        contract_file: Schema contract file name
        
    Returns:
        PySpark StructType
    """
    loader = SchemaContractLoader()
    table_schema = loader.get_table_schema(table_name, contract_file)
    
    fields = []
    
    type_mapping = {
        "STRING": StringType(),
        "INT": IntegerType(),
        "BIGINT": LongType(),
        "BOOLEAN": BooleanType(),
        "TIMESTAMP": TimestampType(),
        "DOUBLE": DoubleType()
    }
    
    for field_name, field_def in table_schema["fields"].items():
        field_type = field_def["type"]
        is_nullable = field_def.get("nullable", False)
        
        spark_type = type_mapping.get(field_type, StringType())
        
        fields.append(StructField(field_name, spark_type, nullable=is_nullable))
    
    return StructType(fields)


def log_validation_results(spark, issues: List[Dict], table_name: str):
    """
    Log validation results to Bronze/Silver/Gold tables for audit trail.
    
    Args:
        spark: SparkSession
        issues: List of validation issues
        table_name: Table name
    """
    if not issues:
        logger.info(f"Schema validation passed for {table_name}")
        return
    
    # Create issues dataframe
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    
    issues_schema = StructType([
        StructField("validation_timestamp", TimestampType(), False),
        StructField("table_name", StringType(), False),
        StructField("field_name", StringType(), False),
        StructField("issue_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("message", StringType(), True),
        StructField("expected_type", StringType(), True),
        StructField("actual_type", StringType(), True),
    ])
    
    issues_rows = [(
        datetime.utcnow(),
        table_name,
        issue.get("field", ""),
        issue.get("issue", ""),
        issue.get("severity", ""),
        issue.get("message", ""),
        issue.get("expected_type", ""),
        issue.get("actual_type", ""),
    ) for issue in issues]
    
    issues_df = spark.createDataFrame(issues_rows, schema=issues_schema)
    
    # Write to audit table (create if not exists)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.schema_audit.validation_issues (
            validation_timestamp TIMESTAMP NOT NULL,
            table_name STRING NOT NULL,
            field_name STRING NOT NULL,
            issue_type STRING NOT NULL,
            severity STRING NOT NULL,
            message STRING,
            expected_type STRING,
            actual_type STRING
        )
        USING iceberg
        PARTITIONED BY (validation_date STRING)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'format-version' = '2'
        )
    """)
    
    # Write with date partition
    issues_df.withColumn(
        "validation_date",
        issues_df["validation_timestamp"].substr(0, 10)  # Extract YYYY-MM-DD
    ).writeTo("s3tablesbucket.schema_audit.validation_issues").append()
    
    logger.info(f"Wrote {len(issues)} schema validation issues to schema_audit.validation_issues")
