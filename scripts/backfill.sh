#!/bin/bash
# =============================================================================
# WikiStream Pipeline - Backfill Script
# =============================================================================
# This script performs idempotent backfill for historical data processing.
# Uses MERGE INTO operations to prevent duplicate records.
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
START_DATE="${1:-}"
END_DATE="${2:-}"
LAYER="${3:-all}"  # bronze, silver, gold, or all
EMR_CLUSTER_ID="${EMR_CLUSTER_ID:-}"
DATA_BUCKET="${DATA_BUCKET:-}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Validate inputs
if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo -e "${RED}Usage: $0 <start_date> <end_date> [layer]${NC}"
    echo -e "  start_date: YYYY-MM-DD format"
    echo -e "  end_date: YYYY-MM-DD format"
    echo -e "  layer: bronze, silver, gold, or all (default: all)"
    exit 1
fi

if [[ -z "$EMR_CLUSTER_ID" ]]; then
    echo -e "${RED}Error: EMR_CLUSTER_ID environment variable not set${NC}"
    exit 1
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}WikiStream Backfill${NC}"
echo -e "${GREEN}Start Date: ${START_DATE}${NC}"
echo -e "${GREEN}End Date: ${END_DATE}${NC}"
echo -e "${GREEN}Layer: ${LAYER}${NC}"
echo -e "${GREEN}========================================${NC}"

# Function to submit EMR step
submit_step() {
    local step_name="$1"
    local spark_args="$2"
    
    echo -e "\n${YELLOW}Submitting step: ${step_name}${NC}"
    
    STEP_ID=$(aws emr add-steps \
        --cluster-id "${EMR_CLUSTER_ID}" \
        --steps Type=Spark,Name="${step_name}",ActionOnFailure=CONTINUE,Args=[${spark_args}] \
        --query 'StepIds[0]' \
        --output text)
    
    echo -e "Step ID: ${STEP_ID}"
    
    # Wait for step completion
    echo -e "${YELLOW}Waiting for step completion...${NC}"
    aws emr wait step-complete \
        --cluster-id "${EMR_CLUSTER_ID}" \
        --step-id "${STEP_ID}"
    
    # Check step status
    STATUS=$(aws emr describe-step \
        --cluster-id "${EMR_CLUSTER_ID}" \
        --step-id "${STEP_ID}" \
        --query 'Step.Status.State' \
        --output text)
    
    if [[ "$STATUS" == "COMPLETED" ]]; then
        echo -e "${GREEN}Step completed successfully${NC}"
        return 0
    else
        echo -e "${RED}Step failed with status: ${STATUS}${NC}"
        return 1
    fi
}

# =============================================================================
# Bronze Layer Backfill
# =============================================================================
if [[ "$LAYER" == "bronze" || "$LAYER" == "all" ]]; then
    echo -e "\n${YELLOW}===== Bronze Layer Backfill =====${NC}"
    echo -e "${YELLOW}Note: Bronze layer typically uses streaming ingestion.${NC}"
    echo -e "${YELLOW}For historical data, use Kafka replay or batch import.${NC}"
    
    # Bronze backfill would require:
    # 1. Kafka topic replay (if data still in retention)
    # 2. Or batch import from external source
    
    echo -e "${YELLOW}Skipping Bronze backfill - use Kafka replay mechanism${NC}"
fi

# =============================================================================
# Silver Layer Backfill
# =============================================================================
if [[ "$LAYER" == "silver" || "$LAYER" == "all" ]]; then
    echo -e "\n${YELLOW}===== Silver Layer Backfill =====${NC}"
    
    # Generate date range
    current_date="$START_DATE"
    while [[ "$current_date" < "$END_DATE" || "$current_date" == "$END_DATE" ]]; do
        echo -e "\nProcessing Silver layer for: ${current_date}"
        
        SPARK_ARGS="--deploy-mode,cluster,\
--master,yarn,\
--conf,spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
--conf,spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog,\
--conf,spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog,\
--packages,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,\
s3://${DATA_BUCKET}/spark/jobs/silver_batch_job.py,\
--bronze-table,s3tablesbucket.bronze.raw_events,\
--silver-namespace,silver,\
--catalog,s3tablesbucket,\
--target-date,${current_date},\
--backfill-mode,true"

        submit_step "Silver-Backfill-${current_date}" "$SPARK_ARGS" || true
        
        # Move to next date
        current_date=$(date -d "$current_date + 1 day" +%Y-%m-%d)
    done
fi

# =============================================================================
# Gold Layer Backfill
# =============================================================================
if [[ "$LAYER" == "gold" || "$LAYER" == "all" ]]; then
    echo -e "\n${YELLOW}===== Gold Layer Backfill =====${NC}"
    
    # Generate date range
    current_date="$START_DATE"
    while [[ "$current_date" < "$END_DATE" || "$current_date" == "$END_DATE" ]]; do
        echo -e "\nProcessing Gold layer for: ${current_date}"
        
        SPARK_ARGS="--deploy-mode,cluster,\
--master,yarn,\
--conf,spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
--conf,spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog,\
--conf,spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog,\
--packages,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,\
s3://${DATA_BUCKET}/spark/jobs/gold_batch_job.py,\
--silver-events-table,s3tablesbucket.silver.cleaned_events,\
--silver-user-activity-table,s3tablesbucket.silver.user_activity,\
--gold-namespace,gold,\
--catalog,s3tablesbucket,\
--target-date,${current_date},\
--backfill-mode,true"

        submit_step "Gold-Backfill-${current_date}" "$SPARK_ARGS" || true
        
        # Move to next date
        current_date=$(date -d "$current_date + 1 day" +%Y-%m-%d)
    done
fi

# =============================================================================
# Summary
# =============================================================================
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Backfill Complete${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\nDate Range: ${START_DATE} to ${END_DATE}"
echo -e "Layer(s): ${LAYER}"

echo -e "\n${YELLOW}Post-Backfill Steps:${NC}"
echo -e "1. Run data quality checks on backfilled data"
echo -e "2. Verify record counts match expectations"
echo -e "3. Check for any duplicate records"
echo -e "4. Refresh QuickSight SPICE datasets"

echo -e "\n${GREEN}Done!${NC}"



