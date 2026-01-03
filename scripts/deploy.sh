#!/bin/bash
# =============================================================================
# WikiStream Pipeline - Deployment Script
# =============================================================================
# Deploys the complete pipeline: Infrastructure → Producer Image → Spark Jobs
#
# Prerequisites:
#   - AWS CLI v2 configured with appropriate credentials
#   - Terraform >= 1.6.0
#   - Docker
#   - jq
#
# Usage:
#   ./deploy.sh                    # Deploy everything
#   ./deploy.sh --infrastructure   # Deploy only Terraform
#   ./deploy.sh --producer         # Build and push producer only
#   ./deploy.sh --spark            # Upload Spark jobs only
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Configuration
TERRAFORM_DIR="${PROJECT_ROOT}/infrastructure/terraform"
PRODUCER_DIR="${PROJECT_ROOT}/producer"
SPARK_DIR="${PROJECT_ROOT}/spark"
CONFIG_DIR="${PROJECT_ROOT}/config"

# Logging functions
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

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
# Start Pipeline
# =============================================================================
start_pipeline() {
    log_info "Starting pipeline components..."
    
    cd "$PROJECT_ROOT"
    
    # Get cluster and service names from outputs
    CLUSTER_NAME=$(jq -r '.ecs_cluster_name.value' outputs.json)
    
    if [[ -z "$CLUSTER_NAME" || "$CLUSTER_NAME" == "null" ]]; then
        log_error "ECS cluster name not found in outputs"
    fi
    
    # Start ECS producer service
    log_info "Starting ECS producer service..."
    aws ecs update-service \
        --cluster "$CLUSTER_NAME" \
        --service "${CLUSTER_NAME%-cluster}-producer" \
        --desired-count 1 \
        --no-cli-pager
    
    # Enable EventBridge rules for batch processing
    log_info "Enabling EventBridge schedules..."
    aws events enable-rule --name "wikistream-dev-silver-schedule" --no-cli-pager || true
    aws events enable-rule --name "wikistream-dev-gold-schedule" --no-cli-pager || true
    
    log_success "Pipeline started"
    
    # Show status
    log_info "Checking producer task status..."
    sleep 5
    aws ecs list-tasks --cluster "$CLUSTER_NAME" --no-cli-pager
}

# =============================================================================
# Show Status
# =============================================================================
show_status() {
    log_info "Pipeline Status"
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
    echo "ECS Producer:"
    if [[ -n "$CLUSTER_NAME" && "$CLUSTER_NAME" != "null" ]]; then
        aws ecs list-tasks --cluster "$CLUSTER_NAME" --no-cli-pager 2>/dev/null || echo "  Unable to query"
    fi
    
    echo ""
    echo "MSK Cluster:"
    if [[ -n "$MSK_ARN" && "$MSK_ARN" != "null" ]]; then
        aws kafka describe-cluster --cluster-arn "$MSK_ARN" \
            --query 'ClusterInfo.State' --output text --no-cli-pager 2>/dev/null || echo "  Unable to query"
    fi
    
    echo ""
    echo "EMR Serverless Application:"
    if [[ -n "$EMR_APP_ID" && "$EMR_APP_ID" != "null" ]]; then
        aws emr-serverless get-application --application-id "$EMR_APP_ID" \
            --query 'application.state' --output text --no-cli-pager 2>/dev/null || echo "  Unable to query"
    fi
    
    echo ""
    echo "============================================="
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo "============================================="
    echo "  WikiStream Pipeline Deployment"
    echo "============================================="
    echo ""
    
    preflight_checks
    
    case "${1:-all}" in
        --infrastructure|-i)
            deploy_infrastructure
            ;;
        --producer|-p)
            deploy_producer
            ;;
        --spark|-s)
            deploy_spark_jobs
            ;;
        --start)
            start_pipeline
            ;;
        --status)
            show_status
            ;;
        all|--all)
            deploy_infrastructure
            deploy_producer
            deploy_spark_jobs
            start_pipeline
            show_status
            ;;
        *)
            echo "Usage: $0 [option]"
            echo ""
            echo "Options:"
            echo "  --all, all         Deploy everything (default)"
            echo "  --infrastructure   Deploy Terraform infrastructure only"
            echo "  --producer         Build and push producer Docker image"
            echo "  --spark            Upload Spark jobs to S3"
            echo "  --start            Start pipeline (ECS + EventBridge)"
            echo "  --status           Show pipeline status"
            exit 1
            ;;
    esac
    
    echo ""
    log_success "Deployment complete!"
}

main "$@"
