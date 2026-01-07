#!/bin/bash
# =============================================================================
# WikiStream - Create Infrastructure (Start of Day)
# =============================================================================
# This script creates ALL infrastructure from scratch including:
# - VPC, NAT Gateway, Security Groups
# - MSK Kafka Cluster (KRaft 3.9.x)
# - EMR Serverless Application
# - ECS Cluster and Service
# - S3 Data Bucket
# - S3 Tables Bucket with optimized maintenance (compaction, snapshots, cleanup)
# - S3 Tables Namespaces (bronze, silver, gold, dq_audit)
# - Step Functions with DQ Gates (Bronze DQ → Silver → Silver DQ → Gold → Gold DQ)
# - EventBridge, Lambda, CloudWatch
# - SNS with email subscription for alerts
# - IAM Roles
#
# After Terraform, it also:
# - Builds and pushes Docker image (if needed)
# - Uploads Spark jobs to S3 (including DQ module)
# - Starts ECS producer service
# - Starts Bronze streaming job
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
TERRAFORM_DIR="${PROJECT_ROOT}/infrastructure/terraform"

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
# Step 1: Terraform Apply
# =============================================================================
echo "🏗️  Step 1/6: Creating infrastructure with Terraform..."
echo "   ⏱️  This takes ~25-35 minutes (MSK cluster creation is slow)"
echo ""

cd "${TERRAFORM_DIR}"

# Initialize Terraform
echo "   Initializing Terraform..."
terraform init -upgrade -input=false

# Validate configuration
echo "   Validating configuration..."
terraform validate

# Apply configuration
echo "   Applying Terraform configuration..."
terraform apply -auto-approve -input=false

echo ""
echo "   ✅ Terraform apply completed!"

# =============================================================================
# Step 2: Get Terraform Outputs
# =============================================================================
echo ""
echo "📤 Step 2/6: Getting Terraform outputs..."

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
echo "   ✅ Outputs saved to outputs.json"

# =============================================================================
# Step 3: Build and Push Docker Image
# =============================================================================
echo ""
echo "🐳 Step 3/6: Checking Docker image..."

# Check if image exists in ECR
IMAGE_COUNT=$(aws ecr list-images --repository-name wikistream-dev-producer --query 'length(imageIds)' --output text 2>/dev/null || echo "0")

if [ "$IMAGE_COUNT" = "0" ] || [ "$IMAGE_COUNT" = "None" ]; then
    echo "   No image found in ECR, building and pushing..."
    
    # Login to ECR
    aws ecr get-login-password --region ${AWS_REGION} | \
        docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
    
    # Build image
    cd "${PROJECT_ROOT}/producer"
    echo "   Building Docker image..."
    docker build --platform linux/amd64 -t ${ECR_REPO}:latest .
    
    # Push image
    echo "   Pushing Docker image to ECR..."
    docker push ${ECR_REPO}:latest
    
    cd "${TERRAFORM_DIR}"
    echo "   ✅ Docker image built and pushed"
else
    echo "   ✅ Docker image already exists in ECR (${IMAGE_COUNT} images)"
fi

# =============================================================================
# Step 4: Upload Spark Jobs to S3
# =============================================================================
echo ""
echo "📦 Step 4/6: Uploading Spark jobs to S3..."

cd "${PROJECT_ROOT}"

# Upload main job files (excluding dq directory)
aws s3 cp spark/jobs/ s3://${DATA_BUCKET}/spark/jobs/ --recursive --quiet \
    --exclude "__pycache__/*" --exclude "*.pyc" --exclude "dq/*"

# Create and upload DQ module as zip (for --py-files)
echo "   Creating DQ module package..."
cd spark/jobs
rm -f dq.zip
zip -rq dq.zip dq/ -x "dq/__pycache__/*" -x "*.pyc"
aws s3 cp dq.zip s3://${DATA_BUCKET}/spark/jobs/dq.zip --quiet
rm -f dq.zip
cd "${PROJECT_ROOT}"

# Upload schemas
aws s3 cp spark/schemas/ s3://${DATA_BUCKET}/spark/schemas/ --recursive --quiet \
    --exclude "__pycache__/*" --exclude "*.pyc"

# Upload config
aws s3 cp config/ s3://${DATA_BUCKET}/config/ --recursive --quiet \
    --exclude "__pycache__/*" --exclude "*.pyc"

# Verify upload
JOB_COUNT=$(aws s3 ls s3://${DATA_BUCKET}/spark/jobs/ --recursive | wc -l | tr -d ' ')
echo "   ✅ Uploaded ${JOB_COUNT} Spark job files (including DQ module)"

# =============================================================================
# Step 5: Start ECS Producer Service
# =============================================================================
echo ""
echo "🔄 Step 5/6: Starting ECS producer service..."

# Update service to desired count 1 and force new deployment
aws ecs update-service \
    --cluster ${ECS_CLUSTER} \
    --service wikistream-dev-producer \
    --desired-count 1 \
    --force-new-deployment \
    --no-cli-pager > /dev/null

echo "   ✅ ECS producer service starting (will be healthy in ~60s)"

# =============================================================================
# Step 6: Start Bronze Streaming Job
# =============================================================================
echo ""
echo "🚀 Step 6/6: Starting Bronze streaming job..."

# Wait for MSK to be fully ready for connections
echo "   Waiting 60s for MSK cluster to stabilize..."
sleep 60

# Start Bronze streaming job
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

echo "   ✅ Bronze streaming job started: ${JOB_RUN_ID}"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "========================================"
echo "🎉 WikiStream Infrastructure READY!"
echo "========================================"
echo ""
echo "📊 Created Resources:"
echo "   • VPC with NAT Gateway"
echo "   • MSK Kafka Cluster (2x kafka.t3.small)"
echo "   • EMR Serverless Application (ID: ${EMR_APP_ID})"
echo "   • ECS Cluster: ${ECS_CLUSTER}"
echo "   • S3 Data Bucket: ${DATA_BUCKET}"
echo "   • S3 Tables Bucket (with auto-maintenance):"
echo "     - Compaction: 512MB target files"
echo "     - Snapshot Management: 7 days retention"
echo "     - Orphan Cleanup: 3 days"
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
echo "⏱️  Pipeline Status:"
echo "   • Producer will connect to MSK in ~1-2 minutes"
echo "   • Bronze job will process first batch in ~2-3 minutes"
echo "   • Data will appear in S3 Tables shortly after"
echo ""
echo "📝 Optional: Enable batch pipeline with DQ gates:"
echo "   aws events enable-rule --name wikistream-dev-batch-pipeline-schedule"
echo ""
echo "📊 Local Monitoring (Grafana):"
echo "   cd monitoring/docker && docker-compose up -d"
echo "   Open http://localhost:3000 (admin/wikistream)"
echo ""
echo "🛑 End of day: ./scripts/destroy_infra.sh"
