#!/bin/bash
# ============================================================================
# WikiStream - Terraform Import Script
# Imports existing AWS resources into Terraform state to prevent conflicts
# ============================================================================

set -e

ACCOUNT_ID="${AWS_ACCOUNT_ID:-028210902207}"
REGION="${AWS_REGION:-us-east-1}"
PREFIX="${TF_VAR_project_name:-wikistream}-${TF_VAR_environment:-dev}"

echo "=========================================="
echo "WikiStream Terraform Import Script"
echo "=========================================="
echo "Account: $ACCOUNT_ID"
echo "Region:  $REGION"
echo "Prefix:  $PREFIX"
echo "=========================================="
echo ""

# Function to safely import a resource
safe_import() {
    local tf_resource=$1
    local aws_id=$2
    local description=$3
    
    echo -n "  $description: "
    
    # Check if already in state
    if terraform state show "$tf_resource" &>/dev/null 2>&1; then
        echo "✓ already in state"
        return 0
    fi
    
    # Try to import
    if terraform import "$tf_resource" "$aws_id" &>/dev/null 2>&1; then
        echo "✓ imported"
        return 0
    else
        echo "- not found or import failed"
        return 0  # Don't fail the script
    fi
}

# Function to check if AWS resource exists
aws_exists() {
    local check_cmd=$1
    eval "$check_cmd" &>/dev/null 2>&1
}

echo "Step 1: Importing IAM Roles..."
echo "----------------------------------------"
safe_import "aws_iam_role.ecs_execution" "${PREFIX}-ecs-execution-role" "ECS Execution Role"
safe_import "aws_iam_role.ecs_task" "${PREFIX}-ecs-task-role" "ECS Task Role"
safe_import "aws_iam_role.emr_serverless" "${PREFIX}-emr-serverless-role" "EMR Serverless Role"
safe_import "aws_iam_role.step_functions" "${PREFIX}-sfn-role" "Step Functions Role"
safe_import "aws_iam_role.eventbridge_sfn" "${PREFIX}-eventbridge-sfn-role" "EventBridge SFN Role"
safe_import "aws_iam_role.bronze_restart_lambda" "${PREFIX}-bronze-restart-lambda-role" "Bronze Restart Lambda Role"

echo ""
echo "Step 2: Importing CloudWatch Log Groups..."
echo "----------------------------------------"
safe_import "aws_cloudwatch_log_group.msk" "/aws/msk/${PREFIX}" "MSK Log Group"
safe_import "aws_cloudwatch_log_group.ecs" "/ecs/${PREFIX}-producer" "ECS Log Group"

echo ""
echo "Step 3: Importing ECR Repository..."
echo "----------------------------------------"
safe_import "aws_ecr_repository.producer" "${PREFIX}-producer" "ECR Repository"

echo ""
echo "Step 4: Importing MSK Configuration..."
echo "----------------------------------------"
MSK_CONFIG_ARN=$(aws kafka list-configurations --query "Configurations[?Name=='${PREFIX}-msk-config'].Arn" --output text 2>/dev/null || echo "")
if [ -n "$MSK_CONFIG_ARN" ] && [ "$MSK_CONFIG_ARN" != "None" ]; then
    safe_import "aws_msk_configuration.wikistream" "$MSK_CONFIG_ARN" "MSK Configuration"
else
    echo "  MSK Configuration: - not found"
fi

echo ""
echo "Step 5: Importing S3 Tables Bucket..."
echo "----------------------------------------"
S3_TABLES_ARN="arn:aws:s3tables:${REGION}:${ACCOUNT_ID}:bucket/${PREFIX}-tables"
if aws s3tables get-table-bucket --table-bucket-arn "$S3_TABLES_ARN" --region "$REGION" &>/dev/null 2>&1; then
    safe_import "aws_s3tables_table_bucket.wikistream" "$S3_TABLES_ARN" "S3 Tables Bucket"
else
    echo "  S3 Tables Bucket: - not found (will be created)"
fi

echo ""
echo "Step 6: Importing Other Resources..."
echo "----------------------------------------"

# SNS Topic
SNS_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${PREFIX}-alerts"
if aws sns get-topic-attributes --topic-arn "$SNS_ARN" &>/dev/null 2>&1; then
    safe_import "aws_sns_topic.alerts" "$SNS_ARN" "SNS Alerts Topic"
else
    echo "  SNS Alerts Topic: - not found"
fi

# S3 Bucket
S3_BUCKET="${PREFIX}-data-${ACCOUNT_ID}"
if aws s3api head-bucket --bucket "$S3_BUCKET" &>/dev/null 2>&1; then
    safe_import "aws_s3_bucket.data" "$S3_BUCKET" "S3 Data Bucket"
else
    echo "  S3 Data Bucket: - not found"
fi

# ECS Cluster
if aws ecs describe-clusters --clusters "${PREFIX}-cluster" --query 'clusters[0].status' --output text 2>/dev/null | grep -q "ACTIVE"; then
    safe_import "aws_ecs_cluster.main" "${PREFIX}-cluster" "ECS Cluster"
else
    echo "  ECS Cluster: - not found"
fi

# EMR Serverless Application
EMR_APP_ID=$(aws emr-serverless list-applications --query "applications[?name=='${PREFIX}-spark'].id" --output text 2>/dev/null | awk '{print $1}' || echo "")
if [ -n "$EMR_APP_ID" ] && [ "$EMR_APP_ID" != "None" ]; then
    safe_import "aws_emrserverless_application.spark" "$EMR_APP_ID" "EMR Serverless App"
else
    echo "  EMR Serverless App: - not found"
fi

# MSK Cluster
MSK_CLUSTER_ARN=$(aws kafka list-clusters-v2 --query "ClusterInfoList[?ClusterName=='${PREFIX}-msk'].ClusterArn" --output text 2>/dev/null || echo "")
if [ -n "$MSK_CLUSTER_ARN" ] && [ "$MSK_CLUSTER_ARN" != "None" ]; then
    safe_import "aws_msk_cluster.wikistream" "$MSK_CLUSTER_ARN" "MSK Cluster"
else
    echo "  MSK Cluster: - not found"
fi

echo ""
echo "=========================================="
echo "Import Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. terraform plan    # Review what will be created/modified"
echo "  2. terraform apply   # Apply changes"
echo ""
