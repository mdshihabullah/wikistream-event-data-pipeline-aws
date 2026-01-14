#!/bin/bash
# =============================================================================
# Setup Terraform Remote Backend (S3 + DynamoDB)
# =============================================================================
# This script creates the required AWS resources for Terraform remote state:
# - S3 bucket for state storage (versioned, encrypted)
# - DynamoDB table for state locking
#
# Run this ONCE before terraform init
# =============================================================================

set -e  # Exit on error
set -u  # Exit on undefined variable

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration - Use environment variables or defaults
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-neuefische}"
PROJECT_NAME="wikistream"

# Export for AWS CLI
export AWS_PROFILE
export AWS_REGION

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Terraform Remote Backend Setup${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "  AWS Profile: ${AWS_PROFILE}"
echo -e "  AWS Region:  ${AWS_REGION}"
echo ""

# Get AWS Account ID
echo -e "${YELLOW}→ Getting AWS Account ID...${NC}"
ACCOUNT_ID=$(aws sts get-caller-identity --profile $AWS_PROFILE --query 'Account' --output text)
if [ -z "$ACCOUNT_ID" ]; then
    echo -e "${RED}✗ Failed to get AWS Account ID. Check your AWS credentials for profile '${AWS_PROFILE}'.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Account ID: ${ACCOUNT_ID}${NC}"
echo ""

# Define resource names
STATE_BUCKET="${PROJECT_NAME}-terraform-state-${ACCOUNT_ID}"
LOCK_TABLE="${PROJECT_NAME}-terraform-locks"

# =============================================================================
# Create S3 Bucket for State Storage
# =============================================================================
echo -e "${YELLOW}→ Creating S3 bucket: ${STATE_BUCKET}${NC}"

if aws s3api head-bucket --bucket "${STATE_BUCKET}" --profile $AWS_PROFILE 2>/dev/null; then
    echo -e "${YELLOW}⚠ Bucket already exists. Skipping creation.${NC}"
else
    aws s3api create-bucket \
        --bucket "${STATE_BUCKET}" \
        --region "${AWS_REGION}" \
        --profile $AWS_PROFILE \
        --no-cli-pager
    echo -e "${GREEN}✓ S3 bucket created${NC}"
fi
echo ""

# Enable versioning
echo -e "${YELLOW}→ Enabling versioning on ${STATE_BUCKET}${NC}"
aws s3api put-bucket-versioning \
    --bucket "${STATE_BUCKET}" \
    --versioning-configuration Status=Enabled \
    --profile $AWS_PROFILE \
    --no-cli-pager
echo -e "${GREEN}✓ Versioning enabled${NC}"
echo ""

# Block public access
echo -e "${YELLOW}→ Blocking public access on ${STATE_BUCKET}${NC}"
aws s3api put-public-access-block \
    --bucket "${STATE_BUCKET}" \
    --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" \
    --profile $AWS_PROFILE \
    --no-cli-pager
echo -e "${GREEN}✓ Public access blocked${NC}"
echo ""

# Enable encryption
echo -e "${YELLOW}→ Enabling encryption on ${STATE_BUCKET}${NC}"
aws s3api put-bucket-encryption \
    --bucket "${STATE_BUCKET}" \
    --server-side-encryption-configuration \
        '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' \
    --profile $AWS_PROFILE \
    --no-cli-pager
echo -e "${GREEN}✓ Encryption enabled (AES-256)${NC}"
echo ""

# =============================================================================
# Create DynamoDB Table for State Locking
# =============================================================================
echo -e "${YELLOW}→ Creating DynamoDB table: ${LOCK_TABLE}${NC}"

if aws dynamodb describe-table --table-name "${LOCK_TABLE}" --region "${AWS_REGION}" --profile $AWS_PROFILE 2>/dev/null >/dev/null; then
    echo -e "${YELLOW}⚠ Table already exists. Skipping creation.${NC}"
else
    aws dynamodb create-table \
        --table-name "${LOCK_TABLE}" \
        --attribute-definitions AttributeName=LockID,AttributeType=S \
        --key-schema AttributeName=LockID,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST \
        --region "${AWS_REGION}" \
        --profile $AWS_PROFILE \
        --no-cli-pager > /dev/null

    echo -e "${YELLOW}⏳ Waiting for table to become active...${NC}"
    aws dynamodb wait table-exists \
        --table-name "${LOCK_TABLE}" \
        --region "${AWS_REGION}" \
        --profile $AWS_PROFILE \
        --no-cli-pager
    echo -e "${GREEN}✓ DynamoDB table created and active${NC}"
fi
echo ""

# =============================================================================
# Summary
# =============================================================================
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ Backend Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Resources created:"
echo "  • S3 Bucket: ${STATE_BUCKET}"
echo "  • DynamoDB Table: ${LOCK_TABLE}"
echo "  • Region: ${AWS_REGION}"
echo ""
echo "Backend configuration (already in main.tf):"
echo ""
echo "  backend \"s3\" {"
echo "    bucket         = \"${STATE_BUCKET}\""
echo "    key            = \"wikistream/terraform.tfstate\""
echo "    region         = \"${AWS_REGION}\""
echo "    dynamodb_table = \"${LOCK_TABLE}\""
echo "    encrypt        = true"
echo "  }"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Run: AWS_PROFILE=${AWS_PROFILE} ./scripts/create_infra.sh"
echo "  OR"
echo "  1. cd infrastructure/terraform"
echo "  2. AWS_PROFILE=${AWS_PROFILE} terraform init"
echo "  3. AWS_PROFILE=${AWS_PROFILE} terraform plan"
echo ""

