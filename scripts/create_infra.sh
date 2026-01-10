#!/bin/bash
# =============================================================================
# WikiStream - Create Infrastructure (Robust - Idempotent)
# =============================================================================
# This script creates ALL infrastructure from scratch and can be run repeatedly.
# It handles:
#   - Stopping EMR if running (required for Terraform updates)
#   - Cancelling existing jobs
#   - Starting EMR after Terraform
#   - All edge cases for repeated runs
#
# Usage:
#   ./scripts/create_infra.sh          # Full create
#   ./scripts/create_infra.sh --force  # Skip confirmations
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
TERRAFORM_DIR="${PROJECT_ROOT}/infrastructure/terraform"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }

echo ""
echo "🟢 CREATING WikiStream Infrastructure"
echo "========================================"
echo ""
echo "This will create:"
echo "  • VPC with NAT Gateway"
echo "  • MSK Kafka Cluster (2 brokers)"
echo "  • EMR Serverless Application"
echo "  • ECS Cluster and Producer Service"
echo "  • S3 Data Bucket"
echo "  • S3 Tables Bucket (with optimized maintenance)"
echo "  • S3 Tables Namespaces (bronze, silver, gold, dq_audit)"
echo "  • Step Functions with DQ Gates"
echo "  • EventBridge, Lambda, SNS (email alerts)"
echo "  • CloudWatch Dashboard and Alarms"
echo ""

AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "📋 Configuration:"
echo "   AWS Account: ${AWS_ACCOUNT_ID}"
echo "   AWS Region:  ${AWS_REGION}"
echo ""

# =============================================================================
# Step 0: Stop EMR if Running (Required for Terraform Updates)
# =============================================================================
echo "🛑 Step 0/7: Preparing EMR Serverless for updates..."

# Find existing EMR app
EMR_APP_ID=$(aws emr-serverless list-applications \
    --query 'applications[?contains(name,`wikistream`)].id' \
    --output text 2>/dev/null || echo "")

if [ -n "$EMR_APP_ID" ]; then
    EMR_STATE=$(aws emr-serverless get-application \
        --application-id "$EMR_APP_ID" \
        --query 'application.state' \
        --output text 2>/dev/null || echo "UNKNOWN")
    
    log_info "Found EMR App: $EMR_APP_ID (State: $EMR_STATE)"
    
    if [ "$EMR_STATE" == "STARTED" ] || [ "$EMR_STATE" == "STARTING" ]; then
        log_info "Cancelling any running jobs..."
        
        # Cancel all running/pending jobs
        JOBS=$(aws emr-serverless list-job-runs \
            --application-id "$EMR_APP_ID" \
            --states RUNNING PENDING SUBMITTED SCHEDULED \
            --query 'jobRuns[*].id' \
            --output text 2>/dev/null || echo "")
        
        for JOB_ID in $JOBS; do
            log_info "  Cancelling job: $JOB_ID"
            aws emr-serverless cancel-job-run \
                --application-id "$EMR_APP_ID" \
                --job-run-id "$JOB_ID" 2>/dev/null || true
        done
        
        # Wait for jobs to cancel
        if [ -n "$JOBS" ]; then
            log_info "Waiting for jobs to cancel..."
            for i in {1..24}; do
                STILL_RUNNING=$(aws emr-serverless list-job-runs \
                    --application-id "$EMR_APP_ID" \
                    --states RUNNING PENDING SUBMITTED CANCELLING \
                    --query 'jobRuns[*].id' \
                    --output text 2>/dev/null || echo "")
                [ -z "$STILL_RUNNING" ] && break
                echo -n "."
                sleep 5
            done
            echo ""
        fi
        
        # Stop EMR application
        log_info "Stopping EMR application..."
        aws emr-serverless stop-application --application-id "$EMR_APP_ID" 2>/dev/null || true
        
        # Wait for STOPPED state
        for i in {1..24}; do
            STATE=$(aws emr-serverless get-application \
                --application-id "$EMR_APP_ID" \
                --query 'application.state' \
                --output text 2>/dev/null || echo "STOPPED")
            [ "$STATE" == "STOPPED" ] || [ "$STATE" == "TERMINATED" ] && break
            echo -n "."
            sleep 5
        done
        echo ""
        log_success "EMR application stopped"
    else
        log_info "EMR already in $EMR_STATE state (no action needed)"
    fi
else
    log_info "No existing EMR application found (fresh install)"
fi

# =============================================================================
# Step 1: Terraform Apply
# =============================================================================
echo ""
echo "🏗️  Step 1/7: Creating infrastructure with Terraform..."
echo "   ⏱️  This takes ~25-35 minutes (MSK cluster creation is slow)"
echo ""

cd "${TERRAFORM_DIR}"

# Initialize Terraform
log_info "Initializing Terraform..."
terraform init -upgrade -input=false

# Validate configuration
log_info "Validating configuration..."
terraform validate

# Apply configuration
log_info "Applying Terraform configuration..."
terraform apply -auto-approve -input=false

log_success "Terraform apply completed!"

# =============================================================================
# Step 1.5: Configure S3 Tables Storage Class
# =============================================================================
echo ""
echo "🗄️  Step 1.5/7: Configuring S3 Tables default storage class..."

S3_TABLES_ARN=$(terraform output -raw s3_tables_bucket_arn)

log_info "Setting Intelligent-Tiering for new tables..."
aws s3tables put-table-bucket-storage-class \
    --table-bucket-arn "${S3_TABLES_ARN}" \
    --storage-class-configuration storageClass=INTELLIGENT_TIERING \
    --region ${AWS_REGION} 2>/dev/null || true

log_success "S3 Tables bucket configured for Intelligent-Tiering"

# =============================================================================
# Step 2: Get Terraform Outputs
# =============================================================================
echo ""
echo "📤 Step 2/7: Getting Terraform outputs..."

# Export outputs to JSON for reference
terraform output -json > "${PROJECT_ROOT}/outputs.json"

# Get individual values
DATA_BUCKET=$(terraform output -raw data_bucket)
ECR_REPO=$(terraform output -raw ecr_repository_url)
ECS_CLUSTER=$(terraform output -raw ecs_cluster_name)
EMR_APP_ID=$(terraform output -raw emr_serverless_app_id)
EMR_ROLE_ARN=$(terraform output -raw emr_serverless_role_arn)
MSK_BOOTSTRAP=$(terraform output -raw msk_bootstrap_brokers_iam)
S3_TABLES_ARN=$(terraform output -raw s3_tables_bucket_arn)

echo "   Data Bucket:    ${DATA_BUCKET}"
echo "   ECR Repository: ${ECR_REPO}"
echo "   ECS Cluster:    ${ECS_CLUSTER}"
echo "   EMR App ID:     ${EMR_APP_ID}"
echo "   S3 Tables ARN:  ${S3_TABLES_ARN}"
log_success "Outputs saved to outputs.json"

# =============================================================================
# Step 3: Build and Push Docker Image
# =============================================================================
echo ""
echo "🐳 Step 3/7: Checking Docker image..."

# Check if image exists in ECR
IMAGE_COUNT=$(aws ecr list-images --repository-name wikistream-dev-producer --query 'length(imageIds)' --output text 2>/dev/null || echo "0")

if [ "$IMAGE_COUNT" = "0" ] || [ "$IMAGE_COUNT" = "None" ]; then
    log_info "No image found in ECR, building and pushing..."
    
    # Login to ECR
    aws ecr get-login-password --region ${AWS_REGION} | \
        docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
    
    # Build image
    cd "${PROJECT_ROOT}/producer"
    log_info "Building Docker image..."
    docker build --no-cache --platform linux/amd64 -t ${ECR_REPO}:latest .
    
    # Push image
    log_info "Pushing Docker image to ECR..."
    docker push ${ECR_REPO}:latest
    
    cd "${TERRAFORM_DIR}"
    log_success "Docker image built and pushed"
else
    log_success "Docker image already exists in ECR (${IMAGE_COUNT} images)"
fi

# =============================================================================
# Step 4: Upload Spark Jobs to S3
# =============================================================================
echo ""
echo "📦 Step 4/7: Uploading Spark jobs to S3..."

cd "${PROJECT_ROOT}"

# Upload main job files (excluding dq directory)
aws s3 cp spark/jobs/ s3://${DATA_BUCKET}/spark/jobs/ --recursive --quiet \
    --exclude "__pycache__/*" --exclude "*.pyc" --exclude "dq/*"

# Create and upload DQ module as zip (for --py-files)
log_info "Creating DQ module package with PyDeequ..."
cd spark/jobs
rm -rf dq_package dq.zip 2>/dev/null || true

# Create package directory
mkdir -p dq_package

# Install pydeequ into the package directory using uv
uv pip install pydeequ==1.4.0 --target dq_package --quiet --no-deps 2>/dev/null || \
    pip install pydeequ==1.4.0 --target dq_package --quiet --no-deps 2>/dev/null || true

# Copy our DQ module
cp -r dq dq_package/

# Create zip from package directory
cd dq_package
zip -rq ../dq.zip . -x "*/__pycache__/*" -x "*.pyc" -x "*.dist-info/*"
cd ..

# Upload to S3
aws s3 cp dq.zip s3://${DATA_BUCKET}/spark/jobs/dq.zip --quiet

# Cleanup
rm -rf dq_package dq.zip
cd "${PROJECT_ROOT}"

# Upload schemas
aws s3 cp spark/schemas/ s3://${DATA_BUCKET}/spark/schemas/ --recursive --quiet \
    --exclude "__pycache__/*" --exclude "*.pyc" 2>/dev/null || true

# Upload config
aws s3 cp config/ s3://${DATA_BUCKET}/config/ --recursive --quiet \
    --exclude "__pycache__/*" --exclude "*.pyc" 2>/dev/null || true

# Verify upload
JOB_COUNT=$(aws s3 ls s3://${DATA_BUCKET}/spark/jobs/ --recursive | wc -l | tr -d ' ')
log_success "Uploaded ${JOB_COUNT} Spark job files (including DQ module)"

# =============================================================================
# Step 5: Start ECS Producer Service
# =============================================================================
echo ""
echo "🔄 Step 5/7: Starting ECS producer service..."

# Update service to desired count 1 and force new deployment
aws ecs update-service \
    --cluster ${ECS_CLUSTER} \
    --service wikistream-dev-producer \
    --desired-count 1 \
    --force-new-deployment \
    --no-cli-pager > /dev/null

log_success "ECS producer service starting (will be healthy in ~60s)"

# =============================================================================
# Step 6: Start Bronze Streaming Job
# =============================================================================
echo ""
echo "🚀 Step 6/7: Starting Bronze streaming job..."

# First, ensure EMR app is STARTED
log_info "Ensuring EMR application is started..."

EMR_STATE=$(aws emr-serverless get-application \
    --application-id "$EMR_APP_ID" \
    --query 'application.state' \
    --output text 2>/dev/null || echo "STOPPED")

if [ "$EMR_STATE" != "STARTED" ]; then
    log_info "Starting EMR application..."
    aws emr-serverless start-application --application-id "$EMR_APP_ID" 2>/dev/null || true
    
    # Wait for STARTED state
    log_info "Waiting for EMR to start..."
    for i in {1..36}; do  # Max 3 minutes
        STATE=$(aws emr-serverless get-application \
            --application-id "$EMR_APP_ID" \
            --query 'application.state' \
            --output text 2>/dev/null || echo "UNKNOWN")
        if [ "$STATE" == "STARTED" ]; then
            log_success "EMR application started"
            break
        fi
        echo -n "."
        sleep 5
    done
    echo ""
fi

# Wait for MSK to be fully ready for connections
log_info "Waiting 30s for MSK cluster to stabilize..."
sleep 30

# Start Bronze streaming job
log_info "Submitting Bronze streaming job..."
JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --application-id ${EMR_APP_ID} \
    --execution-role-arn ${EMR_ROLE_ARN} \
    --name "bronze-streaming" \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'"${DATA_BUCKET}"'/spark/jobs/bronze_streaming_job.py",
            "entryPointArguments": ["'"${MSK_BOOTSTRAP}"'", "'"${DATA_BUCKET}"'"],
            "sparkSubmitParameters": "--conf spark.driver.cores=2 --conf spark.driver.memory=4g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=2 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,software.amazon.msk:aws-msk-iam-auth:2.2.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse='"${S3_TABLES_ARN}"' --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "cloudWatchLoggingConfiguration": {
                "enabled": true,
                "logGroupName": "/aws/emr-serverless/wikistream-dev",
                "logStreamNamePrefix": "bronze"
            },
            "s3MonitoringConfiguration": {
                "logUri": "s3://'"${DATA_BUCKET}"'/emr-serverless/logs/bronze/"
            }
        }
    }' \
    --query 'jobRunId' \
    --output text)

log_success "Bronze streaming job started: ${JOB_RUN_ID}"

# =============================================================================
# Step 7/7: Schedule Batch Pipeline Start (after 15 min for Bronze to collect data)
# =============================================================================
echo ""
echo "⏰ Step 7/7: Scheduling batch pipeline to start in 15 minutes..."

log_info "Bronze streaming needs time to collect data before batch processing can start"
log_info "The batch pipeline will start automatically after 15 minutes"
log_info "Pipeline flow: Bronze DQ → Silver → Silver DQ → Gold → Gold DQ → Wait 10min → Repeat"

# Start the batch pipeline in background after 15 minutes
(
    sleep 900  # Wait 15 minutes
    echo "[$(date)] Starting initial batch pipeline execution..."
    aws stepfunctions start-execution \
        --state-machine-arn "arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:wikistream-dev-batch-pipeline" \
        --input '{"triggered_by": "initial_start", "wait_minutes": 15}' \
        > /dev/null 2>&1 && \
    echo "[$(date)] ✅ Batch pipeline started! It will now run continuously."
) &
BATCH_SCHEDULER_PID=$!
log_success "Batch pipeline scheduler started (PID: ${BATCH_SCHEDULER_PID})"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "========================================"
echo -e "${GREEN}🎉 WikiStream Infrastructure READY!${NC}"
echo "========================================"
echo ""
echo "📊 Created Resources:"
echo "   • VPC with NAT Gateway"
echo "   • MSK Kafka Cluster (2x kafka.t3.small)"
echo "   • EMR Serverless Application (ID: ${EMR_APP_ID})"
echo "   • ECS Cluster: ${ECS_CLUSTER}"
echo "   • S3 Data Bucket: ${DATA_BUCKET}"
echo "   • S3 Tables Bucket (with auto-maintenance & Intelligent-Tiering)"
echo "   • S3 Tables Namespaces: bronze, silver, gold, dq_audit"
echo "   • DQ Gates: Bronze → Silver → Gold (blocks on failure)"
echo ""
echo "🔄 Running Jobs:"
echo "   • ECS Producer: Starting..."
echo "   • Bronze Streaming: ${JOB_RUN_ID}"
echo ""
echo "🔗 Console Links:"
echo "   EMR: https://${AWS_REGION}.console.aws.amazon.com/emr/home?region=${AWS_REGION}#/serverless/applications/${EMR_APP_ID}"
echo "   ECS: https://${AWS_REGION}.console.aws.amazon.com/ecs/v2/clusters/${ECS_CLUSTER}/services"
echo "   Dashboard: https://${AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region=${AWS_REGION}#dashboards:name=wikistream-dev-pipeline-dashboard"
echo ""
echo "⏱️  Pipeline Timeline:"
echo "   • Now:        Producer connects to MSK"
echo "   • +2 min:     Bronze streaming processes first batch"
echo "   • +15 min:    Batch pipeline starts automatically"
echo "   • Continuous: Pipeline loops every ~15-20 min (10 min wait between cycles)"
echo ""
echo "🔄 Batch Pipeline Flow (continuous on success):"
echo "   Bronze DQ → Silver → Silver DQ → Gold → Gold DQ"
echo "   ↓ SUCCESS: Wait 10min → Repeat"
echo "   ↓ FAILURE: Stop loop (manual restart required after fix)"
echo ""
echo "📊 Manual Control:"
echo "   • Start:   aws stepfunctions start-execution --state-machine-arn arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:wikistream-dev-batch-pipeline"
echo "   • Stop:    Use AWS Console to stop the running execution"
echo "   • Restart: After fixing issues, run the Start command above"
echo ""
echo "🛑 End of day: ./scripts/destroy_all.sh"
