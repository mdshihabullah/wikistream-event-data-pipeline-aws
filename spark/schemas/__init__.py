#!/usr/bin/env python3
"""
WikiStream Schemas Module
====================
Centralized schema definitions and validation for all pipeline layers.

Components:
- schema_v1_contract.json: JSON schema contract defining all tables
- schema_registry.py: Schema loader, validator, and drift detector
- schema_validator.py: Helper functions for schema validation

Usage:
    from spark.schemas import SchemaRegistry, SchemaValidator, create_spark_schema_from_contract
"""
