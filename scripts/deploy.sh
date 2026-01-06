#!/bin/bash
# =============================================================================
# WikiStream Pipeline - Deployment Script (Optimized for EMR Serverless)
# =============================================================================
# Deploys the complete pipeline: Infrastructure → Producer Image → Spark Jobs
#
# IMPORTANT: Before first deployment, request EMR Serverless quota increase:
#   1. Go to AWS Console → Service Quotas → EMR Serverless
#   2. Find "Max concurrent vCPUs per account"
#   3. Request increase from 16 to 32 vCPU
#   4. Wait for approval (usually 24-48 hours)
#
# Prerequisites:
#   - AWS CLI v2 configured with appropriate credentials
#   - Terraform >= 1.6.0
#   - Docker
#   - jq
#   - EMR Serverless quota >= 32 vCPU (or 16 with sequential job execution)
#
# Usage:
#   ./deploy.sh                    # Deploy everything
#   ./deploy.sh --infrastructure   # Deploy only Terraform
#   ./deploy.sh --producer         # Build and push producer only
#   ./deploy.sh --spark            # Upload Spark jobs only
#   ./deploy.sh --start            # Start pipeline components
#   ./deploy.sh --quota-check      # Check EMR Serverless quota
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Configuration
TERRAFORM_DIR="${PROJECT_ROOT}/infrastructure/terraform"
PRODUCER_DIR="${PROJECT_ROOT}/producer"
SPARK_DIR="${PROJECT_ROOT}/spark"
CONFIG_DIR="${PROJECT_ROOT}/config"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Logging functions
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }

# =============================================================================
# Pre-flight Checks
# =============================================================================
preflight_checks() {
    log_info "Running pre-flight checks..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI not found. Please install: https://aws.amazon.com/cli/"
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured. Run 'aws configure'"
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        log_error "Terraform not found. Please install: https://terraform.io/downloads"
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker not found. Please install Docker"
    fi
    
    # Check jq
    if ! command -v jq &> /dev/null; then
        log_error "jq not found. Please install: brew install jq"
    fi
    
    log_success "Pre-flight checks passed"
}

# =============================================================================
# EMR Serverless Quota Check
# =============================================================================
check_emr_quota() {
    log_info "Checking EMR Serverless vCPU quota..."
    
    # Try to get current quota
    QUOTA=$(aws service-quotas get-service-quota \
        --service-code emr-serverless \
        --quota-code L-D05C8A75 \
        --region "$AWS_REGION" \
        --query 'Quota.Value' \
        --output text 2>/dev/null || echo "16")
    
    echo ""
    echo "============================================="
    echo "  EMR Serverless vCPU Quota Check"
    echo "============================================="
    echo ""
    echo "  Current quota: ${QUOTA} vCPU"
    echo "  Required:      32 vCPU (recommended) or 16 vCPU (minimum)"
    echo ""
    
    if [[ "${QUOTA%.*}" -ge 32 ]]; then
        log_success "Quota is sufficient (${QUOTA} >= 32 vCPU)"
    elif [[ "${QUOTA%.*}" -ge 16 ]]; then
        log_warn "Quota is at minimum (${QUOTA} vCPU). Jobs will run sequentially."
        log_warn "Consider requesting quota increase to 32 vCPU for better performance."
        echo ""
        echo "  To request quota increase:"
        echo "  1. Go to: https://${AWS_REGION}.console.aws.amazon.com/servicequotas/home/services/emr-serverless/quotas"
        echo "  2. Click 'Max concurrent vCPUs per account'"
        echo "  3. Click 'Request quota increase'"
        echo "  4. Enter 32 (or higher for production)"
        echo ""
    else
        log_error "Quota too low (${QUOTA} vCPU < 16). Request increase before deploying."
    fi
    
    echo "============================================="
}

# =============================================================================
# Start Bronze Streaming Job
# =============================================================================
start_bronze_streaming() {
    log_info "Starting Bronze streaming job on EMR Serverless..."
    
    cd "$PROJECT_ROOT"
    
    if [[ ! -f "outputs.json" ]]; then
        log_error "outputs.json not found. Run deploy_infrastructure first."
    fi
    
    EMR_APP_ID=$(jq -r '.emr_serverless_app_id.value' outputs.json)
    EMR_ROLE_ARN=$(jq -r '.emr_serverless_role_arn.value' outputs.json)
    DATA_BUCKET=$(jq -r '.data_bucket.value' outputs.json)
    S3_TABLES_ARN=$(jq -r '.s3_tables_bucket_arn.value' outputs.json)
    MSK_BOOTSTRAP=$(jq -r '.msk_bootstrap_brokers_iam.value' outputs.json)
    
    if [[ -z "$EMR_APP_ID" || "$EMR_APP_ID" == "null" ]]; then
        log_error "EMR Application ID not found in outputs"
    fi
    
    # Check if Bronze job is already running
    RUNNING=$(aws emr-serverless list-job-runs \
        --application-id "$EMR_APP_ID" \
        --states RUNNING PENDING SUBMITTED \
        --query "jobRuns[?name=='bronze-streaming'].id" \
        --output text 2>/dev/null || echo "")
    
    if [[ -n "$RUNNING" && "$RUNNING" != "None" ]]; then
        log_warn "Bronze streaming job already running: $RUNNING"
        return 0
    fi
    
    # Spark packages for Kafka and Iceberg
    PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,software.amazon.msk:aws-msk-iam-auth:2.2.0"
    
    # Optimized Spark configuration (8 vCPU total: 2 driver + 2 executors × 2 vCPU)
    SPARK_PARAMS="--conf spark.driver.cores=2 \
--conf spark.driver.memory=4g \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=4g \
--conf spark.executor.instances=2 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.jars.packages=${PACKAGES} \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.enabled=true \
--conf spark.sql.iceberg.handle-timestamp-without-timezone=true \
--conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
--conf spark.sql.catalog.s3tablesbucket.warehouse=${S3_TABLES_ARN} \
--conf spark.sql.catalog.s3tablesbucket.client.region=${AWS_REGION}"
    
    log_info "Starting Bronze streaming job..."
    JOB_RUN_ID=$(aws emr-serverless start-job-run \
        --application-id "$EMR_APP_ID" \
        --execution-role-arn "$EMR_ROLE_ARN" \
        --name "bronze-streaming" \
        --job-driver "{
            \"sparkSubmit\": {
                \"entryPoint\": \"s3://${DATA_BUCKET}/spark/jobs/bronze_streaming_job.py\",
                \"entryPointArguments\": [\"${MSK_BOOTSTRAP}\", \"${DATA_BUCKET}\"],
                \"sparkSubmitParameters\": \"${SPARK_PARAMS}\"
            }
        }" \
        --configuration-overrides "{
            \"monitoringConfiguration\": {
                \"cloudWatchLoggingConfiguration\": {
                    \"enabled\": true,
                    \"logGroupName\": \"/aws/emr-serverless/wikistream-dev\",
                    \"logStreamNamePrefix\": \"bronze\"
                },
                \"s3MonitoringConfiguration\": {
                    \"logUri\": \"s3://${DATA_BUCKET}/emr-serverless/logs/bronze/\"
                }
            }
        }" \
        --query 'jobRunId' \
        --output text)
    
    log_success "Bronze streaming job started: $JOB_RUN_ID"
    echo ""
    echo "  Monitor at: https://${AWS_REGION}.console.aws.amazon.com/emr/home?region=${AWS_REGION}#/serverless/applications/${EMR_APP_ID}/job-runs/${JOB_RUN_ID}"
}

# =============================================================================
# Deploy Infrastructure
# =============================================================================
deploy_infrastructure() {
    log_info "Deploying infrastructure with Terraform..."
    
    cd "$TERRAFORM_DIR"
    
    # Initialize
    log_info "Initializing Terraform..."
    terraform init -upgrade
    
    # Plan
    log_info "Planning deployment..."
    terraform plan -out=tfplan
    
    # Apply
    log_info "Applying infrastructure (this may take 20-30 minutes for MSK)..."
    terraform apply tfplan
    
    # Save outputs
    log_info "Saving Terraform outputs..."
    terraform output -json > "${PROJECT_ROOT}/outputs.json"
    
    log_success "Infrastructure deployed successfully"
}

# =============================================================================
# Build and Push Producer Image
# =============================================================================
deploy_producer() {
    log_info "Building and pushing producer Docker image..."
    
    cd "$PROJECT_ROOT"
    
    # Get ECR repository URL from Terraform outputs
    if [[ ! -f "outputs.json" ]]; then
        log_error "outputs.json not found. Run deploy_infrastructure first."
    fi
    
    ECR_URL=$(jq -r '.ecr_repository_url.value' outputs.json)
    AWS_REGION=$(jq -r '.data_bucket.value | split("-")[1]' outputs.json || echo "us-east-1")
    
    if [[ -z "$ECR_URL" || "$ECR_URL" == "null" ]]; then
        log_error "ECR repository URL not found in outputs"
    fi
    
    # Login to ECR
    log_info "Logging into ECR..."
    aws ecr get-login-password --region "$AWS_REGION" | \
        docker login --username AWS --password-stdin "$(echo $ECR_URL | cut -d'/' -f1)"
    
    # Build image
    log_info "Building Docker image..."
    cd "$PRODUCER_DIR"
    docker build -t wikistream-producer:latest .
    
    # Tag and push
    log_info "Tagging and pushing image..."
    docker tag wikistream-producer:latest "${ECR_URL}:latest"
    docker push "${ECR_URL}:latest"
    
    log_success "Producer image pushed to ECR"
}

# =============================================================================
# Upload Spark Jobs
# =============================================================================
deploy_spark_jobs() {
    log_info "Uploading Spark jobs to S3..."
    
    cd "$PROJECT_ROOT"
    
    # Get S3 bucket from outputs
    if [[ ! -f "outputs.json" ]]; then
        log_error "outputs.json not found. Run deploy_infrastructure first."
    fi
    
    DATA_BUCKET=$(jq -r '.data_bucket.value' outputs.json)
    
    if [[ -z "$DATA_BUCKET" || "$DATA_BUCKET" == "null" ]]; then
        log_error "Data bucket not found in outputs"
    fi
    
    # Upload Spark jobs
    log_info "Uploading spark/jobs/..."
    aws s3 sync "${SPARK_DIR}/jobs/" "s3://${DATA_BUCKET}/spark/jobs/" \
        --exclude "__pycache__/*" \
        --exclude "*.pyc" \
        --exclude ".DS_Store"
    
    # Upload schemas
    log_info "Uploading spark/schemas/..."
    aws s3 sync "${SPARK_DIR}/schemas/" "s3://${DATA_BUCKET}/spark/schemas/" \
        --exclude "__pycache__/*" \
        --exclude "*.pyc"
    
    # Upload config
    log_info "Uploading config/..."
    aws s3 sync "${CONFIG_DIR}/" "s3://${DATA_BUCKET}/config/" \
        --exclude "__pycache__/*" \
        --exclude "*.pyc"
    
    log_success "Spark jobs uploaded to s3://${DATA_BUCKET}/"
}

# =============================================================================
# Start Pipeline (All Components)
# =============================================================================
start_pipeline() {
    log_info "Starting WikiStream pipeline components..."
    echo ""
    echo "============================================="
    echo "  Pipeline Architecture (SLA: ≤5 minutes)"
    echo "============================================="
    echo ""
    echo "  1. ECS Producer → Kafka (continuous)"
    echo "  2. Bronze Streaming → Iceberg (30s micro-batches)"
    echo "  3. Batch Pipeline: Silver → DQ → Gold (every 5 min)"
    echo ""
    echo "============================================="
    echo ""
    
    cd "$PROJECT_ROOT"
    
    if [[ ! -f "outputs.json" ]]; then
        log_error "outputs.json not found. Run deploy_infrastructure first."
    fi
    
    # Get resource IDs
    CLUSTER_NAME=$(jq -r '.ecs_cluster_name.value' outputs.json)
    
    if [[ -z "$CLUSTER_NAME" || "$CLUSTER_NAME" == "null" ]]; then
        log_error "ECS cluster name not found in outputs"
    fi
    
    # Step 1: Start ECS producer service
    log_step "1/4 Starting ECS producer service..."
    aws ecs update-service \
        --cluster "$CLUSTER_NAME" \
        --service "${CLUSTER_NAME%-cluster}-producer" \
        --desired-count 1 \
        --no-cli-pager > /dev/null
    log_success "ECS producer service started"
    
    # Step 2: Start Bronze streaming job
    log_step "2/4 Starting Bronze streaming job..."
    start_bronze_streaming
    
    # Step 3: Enable unified batch pipeline schedule
    log_step "3/4 Enabling batch pipeline schedule (every 5 minutes)..."
    aws events enable-rule --name "wikistream-dev-batch-pipeline-schedule" --no-cli-pager 2>/dev/null || \
        log_warn "Batch pipeline schedule not found - may need to run terraform apply"
    log_success "Batch pipeline schedule enabled"
    
    # Step 4: Trigger initial batch pipeline run
    log_step "4/4 Triggering initial batch pipeline execution..."
    BATCH_PIPELINE_ARN=$(jq -r '.batch_pipeline_state_machine_arn.value' outputs.json 2>/dev/null || echo "")
    if [[ -n "$BATCH_PIPELINE_ARN" && "$BATCH_PIPELINE_ARN" != "null" ]]; then
        EXEC_ARN=$(aws stepfunctions start-execution \
            --state-machine-arn "$BATCH_PIPELINE_ARN" \
            --query 'executionArn' \
            --output text 2>/dev/null || echo "")
        if [[ -n "$EXEC_ARN" ]]; then
            log_success "Batch pipeline triggered: ${EXEC_ARN##*/}"
        fi
    fi
    
    echo ""
    log_success "Pipeline started successfully!"
    echo ""
    echo "  Monitor at:"
    echo "  - CloudWatch Dashboard: https://${AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region=${AWS_REGION}#dashboards:name=wikistream-dev-pipeline-dashboard"
    echo "  - EMR Serverless: https://${AWS_REGION}.console.aws.amazon.com/emr/home?region=${AWS_REGION}#/serverless"
    echo "  - Step Functions: https://${AWS_REGION}.console.aws.amazon.com/states/home?region=${AWS_REGION}#/statemachines"
    echo ""
}

# =============================================================================
# Show Status
# =============================================================================
show_status() {
    echo ""
    echo "============================================="
    echo "  WikiStream Pipeline Status"
    echo "============================================="
    
    cd "$PROJECT_ROOT"
    
    if [[ ! -f "outputs.json" ]]; then
        log_warn "outputs.json not found. Infrastructure may not be deployed."
        return
    fi
    
    CLUSTER_NAME=$(jq -r '.ecs_cluster_name.value' outputs.json 2>/dev/null || echo "")
    MSK_ARN=$(jq -r '.msk_cluster_arn.value' outputs.json 2>/dev/null || echo "")
    EMR_APP_ID=$(jq -r '.emr_serverless_app_id.value' outputs.json 2>/dev/null || echo "")
    
    echo ""
    echo "📦 ECS Producer:"
    if [[ -n "$CLUSTER_NAME" && "$CLUSTER_NAME" != "null" ]]; then
        TASKS=$(aws ecs list-tasks --cluster "$CLUSTER_NAME" --query 'taskArns' --output text 2>/dev/null || echo "")
        if [[ -n "$TASKS" && "$TASKS" != "None" ]]; then
            echo "  ✅ Running"
        else
            echo "  ⏸️  Stopped (run --start to begin)"
        fi
    else
        echo "  ❌ Not deployed"
    fi
    
    echo ""
    echo "📨 MSK Cluster:"
    if [[ -n "$MSK_ARN" && "$MSK_ARN" != "null" ]]; then
        STATE=$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" \
            --query 'ClusterInfo.State' --output text --no-cli-pager 2>/dev/null || echo "UNKNOWN")
        if [[ "$STATE" == "ACTIVE" ]]; then
            echo "  ✅ $STATE"
        else
            echo "  ⏳ $STATE"
        fi
    else
        echo "  ❌ Not deployed"
    fi
    
    echo ""
    echo "⚡ EMR Serverless Application:"
    if [[ -n "$EMR_APP_ID" && "$EMR_APP_ID" != "null" ]]; then
        STATE=$(aws emr-serverless get-application --application-id "$EMR_APP_ID" \
            --query 'application.state' --output text --no-cli-pager 2>/dev/null || echo "UNKNOWN")
        echo "  State: $STATE"
        
        # List running jobs
        JOBS=$(aws emr-serverless list-job-runs \
            --application-id "$EMR_APP_ID" \
            --states RUNNING PENDING SUBMITTED \
            --query 'jobRuns[*].[name,state]' \
            --output text 2>/dev/null || echo "")
        
        if [[ -n "$JOBS" && "$JOBS" != "None" ]]; then
            echo "  Running jobs:"
            echo "$JOBS" | while read -r name state; do
                echo "    - $name: $state"
            done
        else
            echo "  Running jobs: None"
        fi
    else
        echo "  ❌ Not deployed"
    fi
    
    echo ""
    echo "📊 Batch Pipeline Schedule:"
    SCHEDULE_STATE=$(aws events describe-rule --name "wikistream-dev-batch-pipeline-schedule" \
        --query 'State' --output text 2>/dev/null || echo "NOT_FOUND")
    if [[ "$SCHEDULE_STATE" == "ENABLED" ]]; then
        echo "  ✅ Enabled (every 5 minutes)"
    elif [[ "$SCHEDULE_STATE" == "DISABLED" ]]; then
        echo "  ⏸️  Disabled"
    else
        echo "  ❌ Not configured"
    fi
    
    echo ""
    echo "============================================="
    echo ""
    echo "📈 Quick Links:"
    echo "  Dashboard: https://${AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region=${AWS_REGION}#dashboards:name=wikistream-dev-pipeline-dashboard"
    echo "  EMR Jobs:  https://${AWS_REGION}.console.aws.amazon.com/emr/home?region=${AWS_REGION}#/serverless/applications/${EMR_APP_ID}"
    echo ""
}

# =============================================================================
# Stop Pipeline
# =============================================================================
stop_pipeline() {
    log_info "Stopping pipeline components..."
    
    cd "$PROJECT_ROOT"
    
    if [[ ! -f "outputs.json" ]]; then
        log_error "outputs.json not found."
    fi
    
    CLUSTER_NAME=$(jq -r '.ecs_cluster_name.value' outputs.json)
    EMR_APP_ID=$(jq -r '.emr_serverless_app_id.value' outputs.json)
    
    # Stop ECS producer
    log_info "Stopping ECS producer..."
    aws ecs update-service \
        --cluster "$CLUSTER_NAME" \
        --service "${CLUSTER_NAME%-cluster}-producer" \
        --desired-count 0 \
        --no-cli-pager > /dev/null 2>&1 || true
    
    # Disable batch pipeline schedule
    log_info "Disabling batch pipeline schedule..."
    aws events disable-rule --name "wikistream-dev-batch-pipeline-schedule" --no-cli-pager 2>/dev/null || true
    
    # Cancel running EMR jobs
    log_info "Cancelling running EMR jobs..."
    RUNNING_JOBS=$(aws emr-serverless list-job-runs \
        --application-id "$EMR_APP_ID" \
        --states RUNNING PENDING SUBMITTED \
        --query 'jobRuns[*].id' \
        --output text 2>/dev/null || echo "")
    
    for JOB_ID in $RUNNING_JOBS; do
        if [[ -n "$JOB_ID" && "$JOB_ID" != "None" ]]; then
            log_info "Cancelling job: $JOB_ID"
            aws emr-serverless cancel-job-run \
                --application-id "$EMR_APP_ID" \
                --job-run-id "$JOB_ID" \
                --no-cli-pager 2>/dev/null || true
        fi
    done
    
    log_success "Pipeline stopped"
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo "============================================="
    echo "  WikiStream Pipeline Deployment"
    echo "  EMR Serverless + Iceberg + MSK"
    echo "============================================="
    echo ""
    
    case "${1:-help}" in
        --quota-check|-q)
            preflight_checks
            check_emr_quota
            ;;
        --infrastructure|-i)
            preflight_checks
            deploy_infrastructure
            ;;
        --producer|-p)
            preflight_checks
            deploy_producer
            ;;
        --spark|-s)
            preflight_checks
            deploy_spark_jobs
            ;;
        --start)
            preflight_checks
            start_pipeline
            ;;
        --stop)
            preflight_checks
            stop_pipeline
            ;;
        --status)
            show_status
            ;;
        --bronze)
            preflight_checks
            start_bronze_streaming
            ;;
        all|--all)
            preflight_checks
            check_emr_quota
            deploy_infrastructure
            deploy_producer
            deploy_spark_jobs
            start_pipeline
            show_status
            ;;
        help|--help|-h|*)
            echo "Usage: $0 [option]"
            echo ""
            echo "Deployment Options:"
            echo "  --all              Deploy everything (infrastructure → producer → spark → start)"
            echo "  --infrastructure   Deploy Terraform infrastructure only"
            echo "  --producer         Build and push producer Docker image"
            echo "  --spark            Upload Spark jobs to S3"
            echo ""
            echo "Pipeline Control:"
            echo "  --start            Start all pipeline components"
            echo "  --stop             Stop all pipeline components"
            echo "  --bronze           Start Bronze streaming job only"
            echo "  --status           Show pipeline status"
            echo ""
            echo "Diagnostics:"
            echo "  --quota-check      Check EMR Serverless vCPU quota"
            echo "  --help             Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 --quota-check   # Check if quota is sufficient before deploying"
            echo "  $0 --all           # Full deployment"
            echo "  $0 --spark --start # Update Spark jobs and restart pipeline"
            echo ""
            echo "Prerequisites:"
            echo "  - AWS CLI v2 configured with credentials"
            echo "  - Terraform >= 1.6.0"
            echo "  - Docker"
            echo "  - EMR Serverless quota >= 16 vCPU (32 recommended)"
            echo ""
            exit 0
            ;;
    esac
    
    echo ""
    log_success "Operation complete!"
}

main "$@"
