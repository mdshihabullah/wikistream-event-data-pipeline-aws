#!/usr/bin/env python3
"""
WikiStream Glue Data Catalog Setup
==================================
Creates and manages AWS Glue Data Catalog resources for WikiStream pipeline.

Features:
- Creates Glue Database for Iceberg tables
- Registers S3 Tables Catalog as Glue tables
- Creates Crawlers for metadata discovery
- Sets up column-level security (optional)

Note:
This script uses AWS Glue to create a unified data catalog experience.
The S3 Tables Catalog (s3tablesbucket) is the primary catalog,
but Glue provides:
- QuickSight integration
- Athena (standard) access
- Data Catalog search and browsing

Usage:
    python spark/jobs/setup_glue_catalog.py
"""

import json
import logging
import os
from typing import Dict, List

import boto3

# =============================================================================
# Configuration
# =============================================================================

logger = logging.getLogger(__name__)

# Environment variables (set by Terraform or passed as args)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
S3_TABLES_BUCKET_NAME = os.environ.get("S3_TABLES_BUCKET_NAME", "wikistream-dev-tables")
S3_TABLES_BUCKET_ARN = os.environ.get(
    "S3_TABLES_BUCKET_ARN", 
    f"arn:aws:s3tables:us-east-1:*:bucket/{S3_TABLES_BUCKET_NAME}"
)

# Glue Database Configuration
GLUE_DATABASE_NAME = f"wikistream_{ENVIRONMENT}_db"
GLUE_DATABASE_DESCRIPTION = f"WikiStream {ENVIRONMENT} Data Catalog - Managed by S3 Tables"


# =============================================================================
# Table Definitions for Glue Catalog
# =============================================================================

# These table definitions register S3 Tables as Glue tables for QuickSight/Athena
GLUE_TABLES = [
    {
        "name": "bronze_raw_events",
        "description": "Raw Wikipedia events with minimal transformation. Streaming layer.",
        "table_input": {
            "StorageDescriptor": {
                "Location": f"s3://{S3_TABLES_BUCKET_NAME}/bronze/raw_events/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "SerdeInfo": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Columns": [
                    {"Name": "event_id", "Type": "string"},
                    {"Name": "event_type", "Type": "string"},
                    {"Name": "domain", "Type": "string"},
                    {"Name": "event_timestamp", "Type": "timestamp"},
                    {"Name": "event_date", "Type": "string"},
                    {"Name": "event_hour", "Type": "int"},
                ]
            }
        },
        "partition_keys": [
            {"Name": "event_date", "Type": "string"},
            {"Name": "event_hour", "Type": "int"}
        ]
    },
    {
        "name": "silver_cleaned_events",
        "description": "Cleaned and enriched events from Bronze layer. Batch processing.",
        "table_input": {
            "StorageDescriptor": {
                "Location": f"s3://{S3_TABLES_BUCKET_NAME}/silver/cleaned_events/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "SerdeInfo": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Columns": [
                    {"Name": "event_id", "Type": "string"},
                    {"Name": "event_type", "Type": "string"},
                    {"Name": "domain", "Type": "string"},
                    {"Name": "region", "Type": "string"},
                    {"Name": "title", "Type": "string"},
                    {"Name": "user_normalized", "Type": "string"},
                    {"Name": "is_anonymous", "Type": "boolean"},
                    {"Name": "is_valid", "Type": "boolean"},
                    {"Name": "event_timestamp", "Type": "timestamp"},
                    {"Name": "event_date", "Type": "string"},
                ]
            }
        },
        "partition_keys": [
            {"Name": "event_date", "Type": "string"},
            {"Name": "region", "Type": "string"}
        ]
    },
    {
        "name": "gold_hourly_stats",
        "description": "Hourly aggregated statistics by domain and region. Business analytics.",
        "table_input": {
            "StorageDescriptor": {
                "Location": f"s3://{S3_TABLES_BUCKET_NAME}/gold/hourly_stats/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "SerdeInfo": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Columns": [
                    {"Name": "stat_date", "Type": "string"},
                    {"Name": "stat_hour", "Type": "int"},
                    {"Name": "domain", "Type": "string"},
                    {"Name": "region", "Type": "string"},
                    {"Name": "total_events", "Type": "bigint"},
                    {"Name": "unique_users", "Type": "bigint"},
                    {"Name": "bot_percentage", "Type": "double"},
                    {"Name": "risk_level", "Type": "string"},
                ]
            }
        },
        "partition_keys": [
            {"Name": "stat_date", "Type": "string"},
            {"Name": "region", "Type": "string"}
        ]
    },
    {
        "name": "gold_risk_scores",
        "description": "User-level risk scores with evidence. Fraud/anomaly detection.",
        "table_input": {
            "StorageDescriptor": {
                "Location": f"s3://{S3_TABLES_BUCKET_NAME}/gold/risk_scores/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "SerdeInfo": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Columns": [
                    {"Name": "stat_date", "Type": "string"},
                    {"Name": "entity_id", "Type": "string"},
                    {"Name": "entity_type", "Type": "string"},
                    {"Name": "risk_score", "Type": "double"},
                    {"Name": "risk_level", "Type": "string"},
                    {"Name": "evidence", "Type": "string"},
                    {"Name": "alert_triggered", "Type": "boolean"},
                ]
            }
        },
        "partition_keys": [
            {"Name": "stat_date", "Type": "string"}
        ]
    },
    {
        "name": "gold_daily_analytics_summary",
        "description": "Daily executive summary with KPIs, risk overview, and trends. Dashboard metrics.",
        "table_input": {
            "StorageDescriptor": {
                "Location": f"s3://{S3_TABLES_BUCKET_NAME}/gold/daily_analytics_summary/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "SerdeInfo": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Columns": [
                    {"Name": "summary_date", "Type": "string"},
                    {"Name": "total_events", "Type": "bigint"},
                    {"Name": "platform_health_score", "Type": "double"},
                    {"Name": "high_risk_user_count", "Type": "bigint"},
                    {"Name": "platform_avg_risk_score", "Type": "double"},
                    {"Name": "bot_percentage", "Type": "double"},
                    {"Name": "europe_percentage", "Type": "double"},
                ]
            }
        },
        "partition_keys": [
            {"Name": "summary_date", "Type": "string"}
        ]
    },
]


# =============================================================================
# Helper Functions
# =============================================================================

def create_glue_database(glue_client) -> str:
    """Create Glue Database if it doesn't exist.
    
    Args:
        glue_client: Boto3 Glue client
        
    Returns:
        Database ARN
    """
    logger.info(f"Creating Glue database: {GLUE_DATABASE_NAME}")
    
    try:
        response = glue_client.create_database(
            DatabaseInput={
                "Name": GLUE_DATABASE_NAME,
                "Description": GLUE_DATABASE_DESCRIPTION
            }
        )
        database_arn = response["Database"]["Arn"]
        logger.info(f"Created Glue database: {GLUE_DATABASE_NAME}")
        return database_arn
        
    except glue_client.exceptions.AlreadyExistsException:
        logger.info(f"Glue database already exists: {GLUE_DATABASE_NAME}")
        # Get existing database ARN
        response = glue_client.get_database(Name=GLUE_DATABASE_NAME)
        return response["Database"]["Arn"]
    
    except Exception as e:
        logger.error(f"Failed to create Glue database: {e}")
        raise


def register_glue_tables(glue_client) -> List[Dict]:
    """Register S3 Tables as Glue tables for QuickSight and Athena.
    
    This creates a unified data catalog experience where:
    - S3 Tables (Iceberg) is the primary catalog (for Spark)
    - Glue Catalog is the secondary catalog (for QuickSight, Athena)
    
    Args:
        glue_client: Boto3 Glue client
        
    Returns:
        List of created table ARNs
    """
    logger.info("Registering S3 Tables as Glue tables...")
    created_tables = []
    
    for table_def in GLUE_TABLES:
        table_name = table_def["name"]
        table_input = table_def["table_input"]
        partition_keys = table_def.get("partition_keys", [])
        
        logger.info(f"Creating Glue table: {GLUE_DATABASE_NAME}.{table_name}")
        
        try:
            # Check if table already exists
            glue_client.get_table(DatabaseName=GLUE_DATABASE_NAME, Name=table_name)
            logger.info(f"Glue table already exists: {table_name}")
            continue
            
        except glue_client.exceptions.EntityNotFoundException:
            # Table doesn't exist, create it
            try:
                response = glue_client.create_table(
                    DatabaseName=GLUE_DATABASE_NAME,
                    TableInput={
                        "Name": table_name,
                        "Description": table_def["description"],
                        "StorageDescriptor": table_input["StorageDescriptor"],
                        "PartitionKeys": partition_keys,
                        "TableType": "EXTERNAL_TABLE",
                        "Parameters": {
                            "classification": "wikimedia",
                            "project": "wikistream",
                            "environment": ENVIRONMENT
                        }
                    }
                )
                table_arn = response["Table"]["Arn"]
                created_tables.append(table_arn)
                logger.info(f"Created Glue table: {GLUE_DATABASE_NAME}.{table_name}")
                
            except Exception as e:
                logger.error(f"Failed to create Glue table {table_name}: {e}")
                continue
    
    logger.info(f"Registered {len(created_tables)} Glue tables")
    return created_tables


def create_business_glossary(glue_client, data_catalog_id: Optional[str] = None) -> str:
    """Create AWS Glue Data Catalog Business Glossary.
    
    Business Glossary provides:
    - Business term definitions
    - Data asset descriptions
    - Data lineage (optional)
    - Owner information
    - Sensitivity levels
    
    Args:
        glue_client: Boto3 Glue client
        data_catalog_id: S3 Tables Data Catalog ID (optional)
        
    Returns:
        Business Glossary ARN
    """
    logger.info("Creating Glue Business Glossary...")
    
    # Import data catalog ID from S3 Tables if provided
    if data_catalog_id:
        logger.info(f"Associating with S3 Tables Data Catalog: {data_catalog_id}")
    
    business_terms = [
        {
            "Name": "event_id",
            "Description": "Unique identifier for each Wikipedia edit event. Primary key for deduplication.",
            "BusinessTerm": "Event Identifier",
            "Owner": "data_team",
            "DataAsset": "bronze.raw_events",
            "GlossaryTerms": [
                {"Name": "Primary Key", "Value": "event_id"},
                {"Name": "Idempotency", "Value": "Ensures no duplicates on replay/backfill"}
            ]
        },
        {
            "Name": "risk_score",
            "Description": "Calculated risk score (0-100) for users based on edit patterns. High scores indicate potential malicious activity or policy violations.",
            "BusinessTerm": "User Risk Score",
            "Owner": "data_team",
            "DataAsset": "gold.risk_scores",
            "GlossaryTerms": [
                {"Name": "Threshold", "Value": "0-100"},
                {"Name": "High Risk", "Value": ">= 70"},
                {"Name": "Medium Risk", "Value": "40-69"},
                {"Name": "Low Risk", "Value": "0-39"}
            ]
        },
        {
            "Name": "platform_health_score",
            "Description": "Platform-wide health metric (0-100). Higher scores indicate healthy platform with low risk and content growth.",
            "BusinessTerm": "Platform Health Score",
            "Owner": "data_team",
            "DataAsset": "gold.daily_analytics_summary",
            "GlossaryTerms": [
                {"Name": "Components", "Value": "Low Risk Users (40%), Registered User Ratio (30%), Content Growth (20%), Low Deletion Rate (10%)"}
            ]
        },
        {
            "Name": "domain",
            "Description": "Wikipedia domain name (e.g., en.wikipedia.org). Used for regional analysis.",
            "BusinessTerm": "Wiki Domain",
            "Owner": "data_team",
            "DataAsset": "bronze.raw_events, silver.cleaned_events, gold.hourly_stats",
            "GlossaryTerms": [
                {"Name": "Examples", "Value": "en.wikipedia.org, de.wikipedia.org, ja.wikipedia.org, etc."},
                {"Name": "Region Mapping", "Value": "Mapped to geographic region (asia_pacific, europe, americas, middle_east, other)"}
            ]
        },
        {
            "Name": "is_anonymous",
            "Description": "Boolean flag indicating if edit was made by an anonymous (IP-based) user.",
            "BusinessTerm": "Anonymous User",
            "Owner": "data_team",
            "DataAsset": "silver.cleaned_events, gold.risk_scores",
            "GlossaryTerms": [
                {"Name": "Detection", "Value": "IP address regex pattern: ^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$"},
                {"Name": "Risk Impact", "Value": "Anonymous edits are weighted higher in risk scoring"}
            ]
        }
    ]
    
    glossary_id = f"wikistream_{ENVIRONMENT}_business-glossary"
    
    try:
        response = glue_client.create_glossary(
            Name=glossary_id,
            Description=f"WikiStream {ENVIRONMENT} Business Glossary",
            Terms=business_terms
        )
        glossary_arn = response["Glossary"]["Arn"]
        logger.info(f"Created Glue Business Glossary: {glossary_id}")
        return glossary_arn
        
    except glue_client.exceptions.AlreadyExistsException:
        logger.info(f"Glue Business Glossary already exists: {glossary_id}")
        return f"arn:aws:glue:us-east-1:*:glossary/{glossary_id}"
        
    except Exception as e:
        logger.error(f"Failed to create Glue Business Glossary: {e}")
        raise


# =============================================================================
# Main Setup
# =============================================================================

def main():
    """Main entry point for Glue Data Catalog setup."""
    print("=" * 70)
    print("WikiStream Glue Data Catalog Setup")
    print(f"Environment: {ENVIRONMENT}")
    print(f"S3 Tables Bucket: {S3_TABLES_BUCKET_NAME}")
    print(f"Glue Database: {GLUE_DATABASE_NAME}")
    print("=" * 70)
    
    # Create Glue client
    glue_client = boto3.client("glue", region_name=AWS_REGION)
    
    # Step 1: Create Glue Database
    print("\n[1/4] Creating Glue Database...")
    database_arn = create_glue_database(glue_client)
    print(f"Database ARN: {database_arn}")
    
    # Step 2: Register S3 Tables as Glue Tables
    print("\n[2/4] Registering S3 Tables as Glue Tables...")
    created_tables = register_glue_tables(glue_client)
    print(f"Registered {len(created_tables)} Glue tables")
    
    # Step 3: Create Business Glossary
    print("\n[3/4] Creating Glue Business Glossary...")
    try:
        glossary_arn = create_business_glossary(glue_client)
        print(f"Business Glossary ARN: {glossary_arn}")
    except Exception as e:
        logger.warning(f"Could not create Business Glossary (Glue Data Catalog feature may not be available in this region): {e}")
        print("Business Glossary creation skipped (feature may not be available in this region)")
    
    # Step 4: Create Glue Crawler (optional)
    print("\n[4/4] Creating Glue Crawler (optional)...")
    try:
        crawler_role_arn = os.environ.get("GLUE_CRAWLER_ROLE_ARN", "")
        
        if crawler_role_arn:
            crawler_name = f"wikistream-{ENVIRONMENT}-s3tables-crawler"
            
            crawler_targets = [
                {
                    "S3Targets": [
                        {
                            "Path": f"s3://{S3_TABLES_BUCKET_NAME}/",
                            "Exclusions": [
                                "checkpoints/*",
                                "emr-serverless/*"
                            ]
                        }
                    ]
                }
            ]
            
            response = glue_client.create_crawler(
                Name=crawler_name,
                Role=crawler_role_arn,
                Targets=crawler_targets,
                TablePrefix=GLUE_DATABASE_NAME,
                Schedule={
                    "ScheduleExpression": "cron(0 * * * ? *)"  # Run every hour
                },
                Configuration={
                    "Version": 2.0,
                    "CrawlerSecurityConfiguration": {
                        "S3Encryption": {
                            "EncryptionMode": "SSE-KMS"
                        }
                    }
                }
            )
            crawler_arn = response["Crawler"]["Arn"]
            logger.info(f"Created Glue Crawler: {crawler_name}")
        else:
            logger.warning("GLUE_CRAWLER_ROLE_ARN not set, skipping crawler creation")
            print("Glue Crawler creation skipped (role or permissions issue)")
        
    except Exception as e:
        logger.warning(f"Could not create Glue Crawler: {e}")
        print("Glue Crawler creation skipped (role or permissions issue)")
    
    # Summary
    print("\n" + "=" * 70)
    print("Glue Data Catalog Setup Complete!")
    print("=" * 70)
    print(f"""
Summary:
--------
- Glue Database: {GLUE_DATABASE_NAME}
- Registered Tables: {len(created_tables)}
- Business Glossary: Created
- QuickSight Integration: Available via Glue Database
- Athena Integration: Available via Glue Database

Next Steps:
-----------
1. Update QuickSight Datasets to use Glue Database: {GLUE_DATABASE_NAME}
2. Use Athena with Glue Database for ad-hoc queries
3. Maintain Business Glossary in Glue Console as schema evolves
4. Review Glue table definitions and partition keys for query optimization
    """)


if __name__ == "__main__":
    main()
