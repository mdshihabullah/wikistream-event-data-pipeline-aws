#!/bin/bash
# =============================================================================
# WikiStream - FULL DESTROY (Robust Complete Teardown)
# =============================================================================
# This script PROPERLY destroys all WikiStream infrastructure:
#   1. Disables EventBridge rules (stops new triggers)
#   2. Stops Step Functions executions
#   3. Cancels ALL EMR jobs and WAITS for cancellation
#   4. Stops EMR application and WAITS for STOPPED state
#   5. Stops ECS services
#   6. Uses Terraform destroy (primary method)
#   7. Falls back to AWS CLI for any leftovers
#   8. Cleans up S3 Tables and data
#   9. Cleans up CloudWatch logs
#
# Usage:
#   ./scripts/destroy_all.sh          # Interactive confirmation
#   ./scripts/destroy_all.sh --force  # Skip confirmation (CI/CD)
#
# Can be run multiple times safely (idempotent)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }
log_step() { echo -e "\n${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "${YELLOW}$1${NC}"; echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# =============================================================================
# CONFIRMATION
# =============================================================================
echo ""
echo "🚨 FULL DESTROY - WikiStream Infrastructure"
echo "============================================="
echo ""
echo "⚠️  WARNING: This will DELETE EVERYTHING including:"
echo "  ✗ All EMR Serverless jobs and application"
echo "  ✗ All S3 Tables data (bronze, silver, gold, dq_audit)"
echo "  ✗ All S3 bucket data"
echo "  ✗ MSK Kafka cluster"
echo "  ✗ VPC and all networking"
echo "  ✗ ECR repository and Docker images"
echo "  ✗ Step Functions and EventBridge rules"
echo "  ✗ CloudWatch logs and dashboards"
echo ""
echo "This action is IRREVERSIBLE!"
echo ""

if [ "$1" != "--force" ]; then
    read -p "Type 'DELETE EVERYTHING' to confirm: " confirm
    if [ "$confirm" != "DELETE EVERYTHING" ]; then
        log_error "Cancelled. You must type 'DELETE EVERYTHING' exactly."
        echo ""
        echo "Tip: Use --force flag to skip confirmation:"
        echo "  ./scripts/destroy_all.sh --force"
        exit 1
    fi
else
    log_warning "Running with --force flag, skipping confirmation"
fi

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null)
if [ -z "$AWS_ACCOUNT_ID" ]; then
    log_error "Failed to get AWS account ID. Check your AWS credentials."
    exit 1
fi
log_info "AWS Account: $AWS_ACCOUNT_ID, Region: $AWS_REGION"

# =============================================================================
# STEP 1: DISABLE EVENTBRIDGE RULES (Stop new triggers)
# =============================================================================
log_step "STEP 1: Disabling EventBridge Rules"

RULES=$(aws events list-rules --name-prefix "wikistream" --query 'Rules[*].Name' --output text 2>/dev/null || echo "")
if [ -n "$RULES" ]; then
    for RULE in $RULES; do
        log_info "Disabling rule: $RULE"
        aws events disable-rule --name "$RULE" 2>/dev/null || true
    done
    log_success "EventBridge rules disabled"
else
    log_info "No EventBridge rules found"
fi

# =============================================================================
# STEP 2: STOP STEP FUNCTIONS EXECUTIONS
# =============================================================================
log_step "STEP 2: Stopping Step Functions Executions"

SFN_ARNS=$(aws stepfunctions list-state-machines --query 'stateMachines[?contains(name,`wikistream`)].stateMachineArn' --output text 2>/dev/null || echo "")
if [ -n "$SFN_ARNS" ]; then
    for SFN_ARN in $SFN_ARNS; do
        SFN_NAME=$(basename "$SFN_ARN")
        log_info "Checking: $SFN_NAME"
        
        # Stop all running executions
        RUNNING_EXECS=$(aws stepfunctions list-executions --state-machine-arn "$SFN_ARN" --status-filter RUNNING --query 'executions[*].executionArn' --output text 2>/dev/null || echo "")
        if [ -n "$RUNNING_EXECS" ]; then
            for EXEC_ARN in $RUNNING_EXECS; do
                log_info "  Stopping execution: $(basename "$EXEC_ARN")"
                aws stepfunctions stop-execution --execution-arn "$EXEC_ARN" --cause "Infrastructure destroy" 2>/dev/null || true
            done
        fi
    done
    log_success "Step Functions executions stopped"
else
    log_info "No Step Functions found"
fi

# =============================================================================
# STEP 3: CANCEL ALL EMR SERVERLESS JOBS AND WAIT
# =============================================================================
log_step "STEP 3: Cancelling EMR Serverless Jobs"

EMR_APPS=$(aws emr-serverless list-applications --query 'applications[?contains(name,`wikistream`)].[id,name,state]' --output text 2>/dev/null || echo "")

if [ -n "$EMR_APPS" ]; then
    while IFS=$'\t' read -r APP_ID APP_NAME APP_STATE; do
        [ -z "$APP_ID" ] && continue
        
        log_info "Processing EMR App: $APP_NAME ($APP_ID) - State: $APP_STATE"
        
        # Cancel all running/pending jobs
        JOBS=$(aws emr-serverless list-job-runs --application-id "$APP_ID" --states RUNNING PENDING SUBMITTED SCHEDULED --query 'jobRuns[*].[id,name,state]' --output text 2>/dev/null || echo "")
        
        if [ -n "$JOBS" ]; then
            while IFS=$'\t' read -r JOB_ID JOB_NAME JOB_STATE; do
                [ -z "$JOB_ID" ] && continue
                log_info "  Cancelling job: $JOB_NAME ($JOB_ID)"
                aws emr-serverless cancel-job-run --application-id "$APP_ID" --job-run-id "$JOB_ID" 2>/dev/null || true
            done <<< "$JOBS"
            
            # Wait for all jobs to be cancelled (max 2 minutes)
            log_info "  Waiting for jobs to cancel..."
            for i in {1..24}; do
                STILL_RUNNING=$(aws emr-serverless list-job-runs --application-id "$APP_ID" --states RUNNING PENDING SUBMITTED CANCELLING --query 'jobRuns[*].id' --output text 2>/dev/null || echo "")
                if [ -z "$STILL_RUNNING" ]; then
                    log_success "  All jobs cancelled"
                    break
                fi
                echo -n "."
                sleep 5
            done
            echo ""
        else
            log_info "  No running jobs found"
        fi
        
    done <<< "$EMR_APPS"
else
    log_info "No EMR Serverless applications found"
fi

# =============================================================================
# STEP 4: STOP EMR SERVERLESS APPLICATIONS AND WAIT
# =============================================================================
log_step "STEP 4: Stopping EMR Serverless Applications"

# Re-fetch in case state changed
EMR_APPS=$(aws emr-serverless list-applications --query 'applications[?contains(name,`wikistream`)].[id,name,state]' --output text 2>/dev/null || echo "")

if [ -n "$EMR_APPS" ]; then
    while IFS=$'\t' read -r APP_ID APP_NAME APP_STATE; do
        [ -z "$APP_ID" ] && continue
        
        if [ "$APP_STATE" == "STARTED" ] || [ "$APP_STATE" == "STARTING" ]; then
            log_info "Stopping: $APP_NAME ($APP_ID)"
            aws emr-serverless stop-application --application-id "$APP_ID" 2>/dev/null || true
            
            # Wait for STOPPED state (max 2 minutes)
            log_info "  Waiting for STOPPED state..."
            for i in {1..24}; do
                STATE=$(aws emr-serverless get-application --application-id "$APP_ID" --query 'application.state' --output text 2>/dev/null || echo "UNKNOWN")
                if [ "$STATE" == "STOPPED" ] || [ "$STATE" == "TERMINATED" ]; then
                    log_success "  Application stopped"
                    break
                fi
                echo -n "."
                sleep 5
            done
            echo ""
        else
            log_info "$APP_NAME already in $APP_STATE state"
        fi
        
    done <<< "$EMR_APPS"
else
    log_info "No EMR applications to stop"
fi

# =============================================================================
# STEP 5: STOP ECS SERVICES
# =============================================================================
log_step "STEP 5: Stopping ECS Services"

ECS_CLUSTERS=$(aws ecs list-clusters --query 'clusterArns[?contains(@,`wikistream`)]' --output text 2>/dev/null || echo "")
if [ -n "$ECS_CLUSTERS" ]; then
    for CLUSTER_ARN in $ECS_CLUSTERS; do
        CLUSTER_NAME=$(basename "$CLUSTER_ARN")
        log_info "Processing cluster: $CLUSTER_NAME"
        
        SERVICES=$(aws ecs list-services --cluster "$CLUSTER_NAME" --query 'serviceArns' --output text 2>/dev/null || echo "")
        if [ -n "$SERVICES" ]; then
            for SVC in $SERVICES; do
                SVC_NAME=$(basename "$SVC")
                log_info "  Stopping service: $SVC_NAME"
                aws ecs update-service --cluster "$CLUSTER_NAME" --service "$SVC_NAME" --desired-count 0 --no-cli-pager 2>/dev/null || true
            done
        fi
    done
    log_success "ECS services stopped"
else
    log_info "No ECS clusters found"
fi

# =============================================================================
# STEP 6: TERRAFORM DESTROY (Primary Method)
# =============================================================================
log_step "STEP 6: Running Terraform Destroy"

cd "$PROJECT_ROOT/infrastructure/terraform"

# Check for remote state lock first
if [ -f "terraform.tfstate" ] || [ -d ".terraform" ]; then
    log_info "Initializing Terraform..."
    terraform init -upgrade -input=false 2>&1 | grep -v "^$" || true
    
    log_info "Running terraform destroy..."
    if terraform destroy -auto-approve -input=false 2>&1; then
        log_success "Terraform destroy completed"
    else
        log_warning "Terraform destroy had errors, continuing with CLI cleanup..."
    fi
else
    log_warning "No Terraform state found, using CLI cleanup only"
fi

cd "$PROJECT_ROOT"

# =============================================================================
# STEP 7: DELETE S3 TABLES (AWS CLI fallback)
# =============================================================================
log_step "STEP 7: Deleting S3 Tables"

TABLE_BUCKET_ARN="arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/wikistream-dev-tables"

if aws s3tables get-table-bucket --table-bucket-arn "$TABLE_BUCKET_ARN" &>/dev/null; then
    for NS in bronze silver gold dq_audit; do
        TABLES=$(aws s3tables list-tables --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NS" --query 'tables[*].name' --output text 2>/dev/null || echo "")
        if [ -n "$TABLES" ]; then
            for TABLE in $TABLES; do
                log_info "Deleting table: ${NS}.${TABLE}"
                aws s3tables delete-table --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NS" --name "$TABLE" 2>/dev/null || true
            done
        fi
        
        # Delete namespace
        aws s3tables delete-namespace --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NS" 2>/dev/null || true
    done
    
    # Delete table bucket
    log_info "Deleting table bucket..."
    aws s3tables delete-table-bucket --table-bucket-arn "$TABLE_BUCKET_ARN" 2>/dev/null || true
    log_success "S3 Tables deleted"
else
    log_info "S3 Tables bucket not found (already deleted)"
fi

# =============================================================================
# STEP 8: DELETE S3 DATA BUCKET (Optimized batch deletion)
# =============================================================================
log_step "STEP 8: Deleting S3 Data Bucket"

DATA_BUCKET="wikistream-dev-data-${AWS_ACCOUNT_ID}"

if aws s3api head-bucket --bucket "$DATA_BUCKET" 2>/dev/null; then
    log_info "Emptying bucket: $DATA_BUCKET"
    
    # Fast delete all current objects
    aws s3 rm "s3://${DATA_BUCKET}" --recursive --quiet 2>/dev/null || true
    
    # Batch delete all versions (much faster than one-by-one)
    log_info "Deleting object versions (batch mode)..."
    
    # Get all versions and delete in batches of 1000
    while true; do
        VERSIONS=$(aws s3api list-object-versions --bucket "$DATA_BUCKET" --max-keys 1000 \
            --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null)
        
        # Check if there are objects to delete
        OBJECT_COUNT=$(echo "$VERSIONS" | jq '.Objects | length' 2>/dev/null || echo "0")
        [ "$OBJECT_COUNT" == "0" ] || [ "$OBJECT_COUNT" == "null" ] && break
        
        # Batch delete
        echo "$VERSIONS" | aws s3api delete-objects --bucket "$DATA_BUCKET" --delete file:///dev/stdin 2>/dev/null || true
        echo -n "."
    done
    echo ""
    
    # Batch delete all delete markers
    log_info "Deleting delete markers..."
    while true; do
        MARKERS=$(aws s3api list-object-versions --bucket "$DATA_BUCKET" --max-keys 1000 \
            --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null)
        
        MARKER_COUNT=$(echo "$MARKERS" | jq '.Objects | length' 2>/dev/null || echo "0")
        [ "$MARKER_COUNT" == "0" ] || [ "$MARKER_COUNT" == "null" ] && break
        
        echo "$MARKERS" | aws s3api delete-objects --bucket "$DATA_BUCKET" --delete file:///dev/stdin 2>/dev/null || true
        echo -n "."
    done
    echo ""
    
    # Delete bucket
    aws s3api delete-bucket --bucket "$DATA_BUCKET" 2>/dev/null && log_success "S3 bucket deleted" || log_warning "Failed to delete bucket (may have remaining objects)"
else
    log_info "S3 bucket not found (already deleted)"
fi

# =============================================================================
# STEP 9: DELETE ECR REPOSITORY
# =============================================================================
log_step "STEP 9: Deleting ECR Repository"

if aws ecr describe-repositories --repository-names "wikistream-dev-producer" &>/dev/null; then
    aws ecr delete-repository --repository-name "wikistream-dev-producer" --force 2>/dev/null
    log_success "ECR repository deleted"
else
    log_info "ECR repository not found (already deleted)"
fi

# =============================================================================
# STEP 10: DELETE CLOUDWATCH RESOURCES
# =============================================================================
log_step "STEP 10: Deleting CloudWatch Resources"

# Delete log groups
LOG_GROUPS=$(aws logs describe-log-groups --log-group-name-prefix "/aws/emr-serverless/wikistream" --query 'logGroups[*].logGroupName' --output text 2>/dev/null || echo "")
if [ -n "$LOG_GROUPS" ]; then
    for LG in $LOG_GROUPS; do
        log_info "Deleting log group: $LG"
        aws logs delete-log-group --log-group-name "$LG" 2>/dev/null || true
    done
fi

# Delete Lambda log groups
LAMBDA_LOGS=$(aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/wikistream" --query 'logGroups[*].logGroupName' --output text 2>/dev/null || echo "")
if [ -n "$LAMBDA_LOGS" ]; then
    for LG in $LAMBDA_LOGS; do
        log_info "Deleting log group: $LG"
        aws logs delete-log-group --log-group-name "$LG" 2>/dev/null || true
    done
fi

# Delete dashboard
aws cloudwatch delete-dashboards --dashboard-names "wikistream-dev-pipeline-dashboard" 2>/dev/null && \
    log_info "Deleted CloudWatch dashboard" || true

# Delete alarms
ALARMS=$(aws cloudwatch describe-alarms --alarm-name-prefix "wikistream" --query 'MetricAlarms[*].AlarmName' --output text 2>/dev/null || echo "")
if [ -n "$ALARMS" ]; then
    aws cloudwatch delete-alarms --alarm-names $ALARMS 2>/dev/null || true
    log_info "Deleted CloudWatch alarms"
fi

log_success "CloudWatch resources deleted"

# =============================================================================
# STEP 11: CLEANUP REMAINING RESOURCES (Fallback)
# =============================================================================
log_step "STEP 11: Cleaning Up Remaining Resources"

# Delete EMR applications
EMR_APPS=$(aws emr-serverless list-applications --query 'applications[?contains(name,`wikistream`)].id' --output text 2>/dev/null || echo "")
if [ -n "$EMR_APPS" ]; then
    for APP_ID in $EMR_APPS; do
        log_info "Deleting EMR app: $APP_ID"
        aws emr-serverless delete-application --application-id "$APP_ID" 2>/dev/null || true
    done
fi

# Delete MSK clusters
MSK_CLUSTERS=$(aws kafka list-clusters --query 'ClusterInfoList[?contains(ClusterName,`wikistream`)].ClusterArn' --output text 2>/dev/null || echo "")
if [ -n "$MSK_CLUSTERS" ]; then
    for CLUSTER_ARN in $MSK_CLUSTERS; do
        log_info "Deleting MSK cluster: $(basename "$CLUSTER_ARN")"
        aws kafka delete-cluster --cluster-arn "$CLUSTER_ARN" 2>/dev/null || true
    done
fi

# Delete ECS clusters (re-fetch to get current state)
ECS_CLUSTERS=$(aws ecs list-clusters --query 'clusterArns[?contains(@,`wikistream`)]' --output text 2>/dev/null || echo "")
if [ -n "$ECS_CLUSTERS" ]; then
    for CLUSTER_ARN in $ECS_CLUSTERS; do
        CLUSTER_NAME=$(basename "$CLUSTER_ARN")
        # Force delete services
        SERVICES=$(aws ecs list-services --cluster "$CLUSTER_NAME" --query 'serviceArns' --output text 2>/dev/null || echo "")
        if [ -n "$SERVICES" ]; then
            for SVC in $SERVICES; do
                aws ecs delete-service --cluster "$CLUSTER_NAME" --service "$(basename "$SVC")" --force --no-cli-pager 2>/dev/null || true
            done
        fi
        aws ecs delete-cluster --cluster "$CLUSTER_NAME" --no-cli-pager 2>/dev/null || true
    done
fi

# Delete Step Functions
SFN_ARNS=$(aws stepfunctions list-state-machines --query 'stateMachines[?contains(name,`wikistream`)].stateMachineArn' --output text 2>/dev/null || echo "")
if [ -n "$SFN_ARNS" ]; then
    for SFN_ARN in $SFN_ARNS; do
        log_info "Deleting state machine: $(basename "$SFN_ARN")"
        aws stepfunctions delete-state-machine --state-machine-arn "$SFN_ARN" 2>/dev/null || true
    done
fi

# Delete EventBridge rules
RULES=$(aws events list-rules --name-prefix "wikistream" --query 'Rules[*].Name' --output text 2>/dev/null || echo "")
if [ -n "$RULES" ]; then
    for RULE in $RULES; do
        # Remove targets first
        TARGETS=$(aws events list-targets-by-rule --rule "$RULE" --query 'Targets[*].Id' --output text 2>/dev/null || echo "")
        if [ -n "$TARGETS" ]; then
            for TARGET in $TARGETS; do
                aws events remove-targets --rule "$RULE" --ids "$TARGET" 2>/dev/null || true
            done
        fi
        aws events delete-rule --name "$RULE" 2>/dev/null || true
    done
fi

# Delete Lambda functions
LAMBDAS=$(aws lambda list-functions --query 'Functions[?contains(FunctionName,`wikistream`)].FunctionName' --output text 2>/dev/null || echo "")
if [ -n "$LAMBDAS" ]; then
    for FUNC in $LAMBDAS; do
        log_info "Deleting Lambda: $FUNC"
        aws lambda delete-function --function-name "$FUNC" 2>/dev/null || true
    done
fi

# Delete SNS topics
SNS_TOPICS=$(aws sns list-topics --query 'Topics[?contains(TopicArn,`wikistream`)].TopicArn' --output text 2>/dev/null || echo "")
if [ -n "$SNS_TOPICS" ]; then
    for TOPIC in $SNS_TOPICS; do
        log_info "Deleting SNS topic: $(basename "$TOPIC")"
        aws sns delete-topic --topic-arn "$TOPIC" 2>/dev/null || true
    done
fi

log_success "Cleanup completed"

# =============================================================================
# STEP 12: CLEAN LOCAL TERRAFORM STATE (Keep .terraform for caching)
# =============================================================================
log_step "STEP 12: Cleaning Local Terraform State"

cd "$PROJECT_ROOT/infrastructure/terraform"
if [ -f "terraform.tfstate" ]; then
    log_info "Removing local Terraform state files..."
    rm -f terraform.tfstate terraform.tfstate.backup 2>/dev/null || true
    # Note: Keep .terraform directory for provider caching (speeds up next init)
    log_success "Terraform state cleaned"
else
    log_info "No local Terraform state to clean"
fi

# Remove outputs.json if exists
rm -f "$PROJECT_ROOT/outputs.json" 2>/dev/null || true

# =============================================================================
# COMPLETE
# =============================================================================
echo ""
echo "========================================"
echo -e "${GREEN}✅ FULL DESTROY COMPLETED${NC}"
echo "========================================"
echo ""
echo "All WikiStream resources have been deleted."
echo ""
echo "Note: Some resources (MSK, NAT Gateway) may take"
echo "a few minutes to fully terminate in the background."
echo ""
echo "To recreate from scratch:"
echo "  ./scripts/create_infra.sh"
echo ""
