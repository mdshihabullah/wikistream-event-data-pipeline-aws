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
#   ./scripts/destroy_all.sh              # Destroy dev environment (default)
#   ./scripts/destroy_all.sh staging      # Destroy staging environment
#   ./scripts/destroy_all.sh prod         # Destroy prod environment
#   ./scripts/destroy_all.sh dev --force  # Skip confirmation (CI/CD)
#
# Can be run multiple times safely (idempotent)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-neuefische}"

# Parse Arguments
ENVIRONMENT="dev"
FORCE_FLAG=""

for arg in "$@"; do
    case $arg in
        dev|staging|prod)
            ENVIRONMENT="$arg"
            ;;
        --force)
            FORCE_FLAG="--force"
            ;;
        *)
            echo "Unknown argument: $arg"
            echo "Usage: $0 [dev|staging|prod] [--force]"
            exit 1
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    echo "Invalid environment: $ENVIRONMENT"
    echo "Valid environments: dev, staging, prod"
    exit 1
fi

# Set name prefix based on environment
NAME_PREFIX="wikistream-${ENVIRONMENT}"

# Export AWS_PROFILE so all AWS CLI commands inherit it
export AWS_PROFILE
export AWS_REGION
export ENVIRONMENT

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

# Cleanup function for temporary files
cleanup_temp_files() {
    rm -f /tmp/wikistream-delete-*.json 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup_temp_files EXIT INT TERM

# =============================================================================
# CONFIRMATION
# =============================================================================
echo ""
echo "🚨 FULL DESTROY - WikiStream Infrastructure (${ENVIRONMENT})"
echo "============================================="
echo ""
echo "Environment: ${ENVIRONMENT}"
echo "Name Prefix: ${NAME_PREFIX}"
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

if [ -z "$FORCE_FLAG" ]; then
    # Convert environment to uppercase for confirmation
    ENV_UPPER=$(echo "${ENVIRONMENT}" | tr '[:lower:]' '[:upper:]')
    echo -n "Type 'DELETE ${ENV_UPPER}' to confirm: "
    read confirm
    if [ "$confirm" != "DELETE ${ENV_UPPER}" ]; then
        log_error "Cancelled. You must type 'DELETE ${ENV_UPPER}' exactly."
        echo ""
        echo "Tip: Use --force flag to skip confirmation:"
        echo "  ./scripts/destroy_all.sh ${ENVIRONMENT} --force"
        exit 1
    fi
else
    log_warning "Running with --force flag, skipping confirmation"
fi

# Get AWS account ID with profile
log_info "Using AWS Profile: ${AWS_PROFILE}"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --profile $AWS_PROFILE --query Account --output text 2>/dev/null)
if [ -z "$AWS_ACCOUNT_ID" ]; then
    log_error "Failed to get AWS account ID. Check your AWS credentials for profile '${AWS_PROFILE}'."
    exit 1
fi
log_info "AWS Account: $AWS_ACCOUNT_ID, Region: $AWS_REGION, Profile: $AWS_PROFILE"

# Define backend configuration (must match create_infra.sh)
STATE_BUCKET="wikistream-terraform-state-${AWS_ACCOUNT_ID}"
LOCK_TABLE="wikistream-terraform-locks"

# Track deletion status for final summary
declare -A DELETION_STATUS
DELETION_STATUS["eventbridge"]="pending"
DELETION_STATUS["stepfunctions"]="pending"
DELETION_STATUS["emr_jobs"]="pending"
DELETION_STATUS["emr_app"]="pending"
DELETION_STATUS["ecs"]="pending"
DELETION_STATUS["terraform"]="pending"
DELETION_STATUS["s3_tables"]="pending"
DELETION_STATUS["s3_bucket"]="pending"
DELETION_STATUS["ecr"]="pending"
DELETION_STATUS["cloudwatch"]="pending"

# =============================================================================
# STEP 1: DISABLE EVENTBRIDGE (Rules + Scheduler)
# =============================================================================
log_step "STEP 1: Disabling EventBridge Rules & Schedules"

# Delete EventBridge Scheduler schedules (one-time triggers)
SCHEDULES=$(aws scheduler list-schedules --profile $AWS_PROFILE --name-prefix "wikistream" --query 'Schedules[*].Name' --output text 2>/dev/null || echo "")
if [ -n "$SCHEDULES" ]; then
    for SCHEDULE in $SCHEDULES; do
        log_info "Deleting schedule: $SCHEDULE"
        aws scheduler delete-schedule --profile $AWS_PROFILE --name "$SCHEDULE" 2>/dev/null || true
    done
    log_success "EventBridge schedules deleted"
else
    log_info "No EventBridge schedules found"
fi

# Disable EventBridge Rules
RULES=$(aws events list-rules --profile $AWS_PROFILE --name-prefix "wikistream" --query 'Rules[*].Name' --output text 2>/dev/null || echo "")
if [ -n "$RULES" ]; then
    for RULE in $RULES; do
        log_info "Disabling rule: $RULE"
        aws events disable-rule --profile $AWS_PROFILE --name "$RULE" 2>/dev/null || true
    done
    log_success "EventBridge rules disabled"
else
    log_info "No EventBridge rules found"
fi
DELETION_STATUS["eventbridge"]="done"

# =============================================================================
# STEP 2: STOP STEP FUNCTIONS EXECUTIONS
# =============================================================================
log_step "STEP 2: Stopping Step Functions Executions"

SFN_ARNS=$(aws stepfunctions list-state-machines --profile $AWS_PROFILE --query "stateMachines[?contains(name,\`${NAME_PREFIX}\`)].stateMachineArn" --output text 2>/dev/null || echo "")
if [ -n "$SFN_ARNS" ]; then
    for SFN_ARN in $SFN_ARNS; do
        SFN_NAME=$(basename "$SFN_ARN")
        log_info "Checking: $SFN_NAME"
        
        # Stop all running executions
        RUNNING_EXECS=$(aws stepfunctions list-executions --profile $AWS_PROFILE --state-machine-arn "$SFN_ARN" --status-filter RUNNING --query 'executions[*].executionArn' --output text 2>/dev/null || echo "")
        if [ -n "$RUNNING_EXECS" ]; then
            for EXEC_ARN in $RUNNING_EXECS; do
                log_info "  Stopping execution: $(basename "$EXEC_ARN")"
                aws stepfunctions stop-execution --profile $AWS_PROFILE --execution-arn "$EXEC_ARN" --cause "Infrastructure destroy" 2>/dev/null || true
            done
        fi
    done
    log_success "Step Functions executions stopped"
else
    log_info "No Step Functions found"
fi
DELETION_STATUS["stepfunctions"]="done"

# =============================================================================
# STEP 3: CANCEL ALL EMR SERVERLESS JOBS AND WAIT
# =============================================================================
log_step "STEP 3: Cancelling EMR Serverless Jobs"

EMR_APPS=$(aws emr-serverless list-applications --profile $AWS_PROFILE --query "applications[?contains(name,\`${NAME_PREFIX}\`)].[id,name,state]" --output text 2>/dev/null || echo "")

if [ -n "$EMR_APPS" ]; then
    while IFS=$'\t' read -r APP_ID APP_NAME APP_STATE; do
        [ -z "$APP_ID" ] && continue
        
        log_info "Processing EMR App: $APP_NAME ($APP_ID) - State: $APP_STATE"
        
        # Cancel all running/pending jobs
        JOBS=$(aws emr-serverless list-job-runs --profile $AWS_PROFILE --application-id "$APP_ID" --states RUNNING PENDING SUBMITTED SCHEDULED --query 'jobRuns[*].[id,name,state]' --output text 2>/dev/null || echo "")
        
        if [ -n "$JOBS" ]; then
            while IFS=$'\t' read -r JOB_ID JOB_NAME JOB_STATE; do
                [ -z "$JOB_ID" ] && continue
                log_info "  Cancelling job: $JOB_NAME ($JOB_ID)"
                aws emr-serverless cancel-job-run --profile $AWS_PROFILE --application-id "$APP_ID" --job-run-id "$JOB_ID" 2>/dev/null || true
            done <<< "$JOBS"
            
            # Wait for all jobs to be cancelled (max 2 minutes)
            log_info "  Waiting for jobs to cancel..."
            for i in {1..24}; do
                STILL_RUNNING=$(aws emr-serverless list-job-runs --profile $AWS_PROFILE --application-id "$APP_ID" --states RUNNING PENDING SUBMITTED CANCELLING --query 'jobRuns[*].id' --output text 2>/dev/null || echo "")
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
DELETION_STATUS["emr_jobs"]="done"

# =============================================================================
# STEP 4: STOP EMR SERVERLESS APPLICATIONS AND WAIT
# =============================================================================
log_step "STEP 4: Stopping EMR Serverless Applications"

# Re-fetch in case state changed
EMR_APPS=$(aws emr-serverless list-applications --profile $AWS_PROFILE --query "applications[?contains(name,\`${NAME_PREFIX}\`)].[id,name,state]" --output text 2>/dev/null || echo "")

if [ -n "$EMR_APPS" ]; then
    while IFS=$'\t' read -r APP_ID APP_NAME APP_STATE; do
        [ -z "$APP_ID" ] && continue
        
        if [ "$APP_STATE" == "STARTED" ] || [ "$APP_STATE" == "STARTING" ]; then
            log_info "Stopping: $APP_NAME ($APP_ID)"
            aws emr-serverless stop-application --profile $AWS_PROFILE --application-id "$APP_ID" 2>/dev/null || true
            
            # Wait for STOPPED state (max 2 minutes)
            log_info "  Waiting for STOPPED state..."
            for i in {1..24}; do
                STATE=$(aws emr-serverless get-application --profile $AWS_PROFILE --application-id "$APP_ID" --query 'application.state' --output text 2>/dev/null || echo "UNKNOWN")
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
DELETION_STATUS["emr_app"]="done"

# =============================================================================
# STEP 5: STOP ECS SERVICES
# =============================================================================
log_step "STEP 5: Stopping ECS Services"

ECS_CLUSTERS=$(aws ecs list-clusters --profile $AWS_PROFILE --query "clusterArns[?contains(@,\`${NAME_PREFIX}\`)]" --output text 2>/dev/null || echo "")
if [ -n "$ECS_CLUSTERS" ]; then
    for CLUSTER_ARN in $ECS_CLUSTERS; do
        CLUSTER_NAME=$(basename "$CLUSTER_ARN")
        log_info "Processing cluster: $CLUSTER_NAME"
        
        SERVICES=$(aws ecs list-services --profile $AWS_PROFILE --cluster "$CLUSTER_NAME" --query 'serviceArns' --output text 2>/dev/null || echo "")
        if [ -n "$SERVICES" ]; then
            for SVC in $SERVICES; do
                SVC_NAME=$(basename "$SVC")
                log_info "  Stopping service: $SVC_NAME"
                aws ecs update-service --profile $AWS_PROFILE --cluster "$CLUSTER_NAME" --service "$SVC_NAME" --desired-count 0 --no-cli-pager 2>/dev/null || true
            done
        fi
    done
    log_success "ECS services stopped"
else
    log_info "No ECS clusters found"
fi
DELETION_STATUS["ecs"]="done"

# =============================================================================
# STEP 6: TERRAFORM DESTROY (Primary Method)
# =============================================================================
log_step "STEP 6: Running Terraform Destroy"

cd "$PROJECT_ROOT/infrastructure/terraform"

# Check if backend bucket exists (indicates Terraform state may exist)
BACKEND_EXISTS=$(aws s3api head-bucket --bucket "${STATE_BUCKET}" --profile $AWS_PROFILE --no-cli-pager 2>/dev/null && echo "yes" || echo "no")

if [ "$BACKEND_EXISTS" == "yes" ] || [ -d ".terraform" ]; then
    log_info "Initializing Terraform with backend: s3://${STATE_BUCKET}"
    
    # Initialize with dynamic backend configuration (same as create_infra.sh)
    INIT_OUTPUT=$(terraform init -upgrade -input=false -reconfigure \
        -backend-config="bucket=${STATE_BUCKET}" \
        -backend-config="key=wikistream/${ENVIRONMENT}/terraform.tfstate" \
        -backend-config="region=${AWS_REGION}" \
        -backend-config="dynamodb_table=${LOCK_TABLE}" \
        -backend-config="encrypt=true" 2>&1)
    INIT_EXIT_CODE=$?
    
    # Show output (filter empty lines)
    echo "$INIT_OUTPUT" | grep -v "^$" || true
    
    if [ $INIT_EXIT_CODE -eq 0 ]; then
        log_info "Running terraform destroy for ${ENVIRONMENT} environment..."
        TFVARS_FILE="environments/${ENVIRONMENT}.tfvars"
        if [ -f "$TFVARS_FILE" ]; then
            TFVARS_ARG="-var-file=${TFVARS_FILE}"
        else
            TFVARS_ARG=""
        fi
        
        # Run terraform destroy (output to terminal, not piped)
        terraform destroy -auto-approve -input=false \
            ${TFVARS_ARG} \
            -var="aws_current_profile=${AWS_PROFILE}" \
            -var="aws_region=${AWS_REGION}"
        
        if [ $? -eq 0 ]; then
            log_success "Terraform destroy completed"
            DELETION_STATUS["terraform"]="done"
        else
            log_warning "Terraform destroy had errors, continuing with CLI cleanup..."
            DELETION_STATUS["terraform"]="partial"
        fi
    else
        log_warning "Terraform init failed (exit code: $INIT_EXIT_CODE), using CLI cleanup only"
        DELETION_STATUS["terraform"]="skipped"
    fi
else
    log_warning "No Terraform backend found, using CLI cleanup only"
    DELETION_STATUS["terraform"]="skipped"
fi

cd "$PROJECT_ROOT"

# =============================================================================
# STEP 7: DELETE S3 TABLES (AWS CLI fallback)
# =============================================================================
log_step "STEP 7: Deleting S3 Tables"

TABLE_BUCKET_ARN="arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${NAME_PREFIX}-tables"
S3_TABLES_DELETED=0
S3_TABLES_TOTAL=0

if aws s3tables get-table-bucket --profile $AWS_PROFILE --table-bucket-arn "$TABLE_BUCKET_ARN" &>/dev/null; then
    log_info "S3 Tables bucket found, deleting tables and namespaces..."
    log_info "Note: S3 Tables deletion can take several minutes per table"
    
    for NS in bronze silver gold dq_audit; do
        TABLES=$(aws s3tables list-tables --profile $AWS_PROFILE --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NS" --query 'tables[*].name' --output text 2>/dev/null || echo "")
        if [ -n "$TABLES" ] && [ "$TABLES" != "None" ]; then
            for TABLE in $TABLES; do
                S3_TABLES_TOTAL=$((S3_TABLES_TOTAL + 1))
                log_info "Deleting table: ${NS}.${TABLE} (may take 1-2 min)..."
                if aws s3tables delete-table --profile $AWS_PROFILE --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NS" --name "$TABLE" 2>/dev/null; then
                    S3_TABLES_DELETED=$((S3_TABLES_DELETED + 1))
                    log_success "  Table ${NS}.${TABLE} deleted"
                else
                    log_warning "  Failed to delete ${NS}.${TABLE} (may already be deleted)"
                fi
            done
        fi
        
        # Delete namespace (wait a moment for table deletions to propagate)
        sleep 2
        log_info "Deleting namespace: ${NS}..."
        aws s3tables delete-namespace --profile $AWS_PROFILE --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NS" 2>/dev/null || true
    done
    
    # Wait for table deletions to fully propagate before deleting bucket
    log_info "Waiting for table deletions to propagate (30s)..."
    sleep 30
    
    # Delete table bucket
    log_info "Deleting table bucket..."
    if aws s3tables delete-table-bucket --profile $AWS_PROFILE --table-bucket-arn "$TABLE_BUCKET_ARN" 2>/dev/null; then
        log_success "S3 Tables bucket deleted (${S3_TABLES_DELETED}/${S3_TABLES_TOTAL} tables)"
        DELETION_STATUS["s3_tables"]="done"
    else
        log_warning "S3 Tables bucket deletion initiated (may complete asynchronously)"
        DELETION_STATUS["s3_tables"]="pending_async"
    fi
else
    log_info "S3 Tables bucket not found (already deleted)"
    DELETION_STATUS["s3_tables"]="done"
fi

# =============================================================================
# STEP 8: DELETE S3 DATA BUCKET (Optimized batch deletion)
# =============================================================================
log_step "STEP 8: Deleting S3 Data Bucket"

DATA_BUCKET="${NAME_PREFIX}-data-${AWS_ACCOUNT_ID}"

if aws s3api head-bucket --profile $AWS_PROFILE --bucket "$DATA_BUCKET" --no-cli-pager 2>/dev/null; then
    log_info "Emptying bucket: $DATA_BUCKET"
    
    # Fast delete all current objects (with timeout protection)
    timeout 300 aws s3 rm "s3://${DATA_BUCKET}" --profile $AWS_PROFILE --recursive --quiet 2>/dev/null || true
    
    # Batch delete all versions (much faster than one-by-one)
    log_info "Deleting object versions (batch mode)..."
    
    # Get all versions and delete in batches of 1000
    while true; do
        VERSIONS=$(aws s3api list-object-versions --profile $AWS_PROFILE --bucket "$DATA_BUCKET" --max-keys 1000 \
            --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null)
        
        # Check if there are objects to delete
        OBJECT_COUNT=$(echo "$VERSIONS" | jq '.Objects | length' 2>/dev/null || echo "0")
        [ "$OBJECT_COUNT" == "0" ] || [ "$OBJECT_COUNT" == "null" ] && break
        
        # Batch delete using temporary file to avoid stdin hanging
        TEMP_FILE=$(mktemp /tmp/wikistream-delete-XXXXXX.json)
        echo "$VERSIONS" > "$TEMP_FILE"
        aws s3api delete-objects --profile $AWS_PROFILE --bucket "$DATA_BUCKET" --delete "file://${TEMP_FILE}" --no-cli-pager 2>/dev/null || true
        rm -f "$TEMP_FILE"
        echo -n "."
    done
    echo ""
    
    # Batch delete all delete markers
    log_info "Deleting delete markers..."
    while true; do
        MARKERS=$(aws s3api list-object-versions --profile $AWS_PROFILE --bucket "$DATA_BUCKET" --max-keys 1000 \
            --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null)
        
        MARKER_COUNT=$(echo "$MARKERS" | jq '.Objects | length' 2>/dev/null || echo "0")
        [ "$MARKER_COUNT" == "0" ] || [ "$MARKER_COUNT" == "null" ] && break
        
        # Batch delete using temporary file to avoid stdin hanging
        TEMP_FILE=$(mktemp /tmp/wikistream-delete-XXXXXX.json)
        echo "$MARKERS" > "$TEMP_FILE"
        aws s3api delete-objects --profile $AWS_PROFILE --bucket "$DATA_BUCKET" --delete "file://${TEMP_FILE}" --no-cli-pager 2>/dev/null || true
        rm -f "$TEMP_FILE"
        echo -n "."
    done
    echo ""
    
    # Delete bucket
    if aws s3api delete-bucket --profile $AWS_PROFILE --bucket "$DATA_BUCKET" 2>/dev/null; then
        log_success "S3 bucket deleted"
        DELETION_STATUS["s3_bucket"]="done"
    else
        log_warning "Failed to delete bucket (may have remaining objects)"
        DELETION_STATUS["s3_bucket"]="partial"
    fi
else
    log_info "S3 bucket not found (already deleted)"
    DELETION_STATUS["s3_bucket"]="done"
fi

# =============================================================================
# STEP 9: DELETE ECR REPOSITORY
# =============================================================================
log_step "STEP 9: Deleting ECR Repository"

# Use environment-specific name
ECR_REPO_NAME="${NAME_PREFIX}-producer"
if aws ecr describe-repositories --profile $AWS_PROFILE --repository-names "$ECR_REPO_NAME" --no-cli-pager &>/dev/null; then
    if aws ecr delete-repository --profile $AWS_PROFILE --repository-name "$ECR_REPO_NAME" --force --no-cli-pager 2>/dev/null; then
        log_success "ECR repository deleted"
        DELETION_STATUS["ecr"]="done"
    else
        log_warning "Failed to delete ECR repository"
        DELETION_STATUS["ecr"]="failed"
    fi
else
    log_info "ECR repository not found (already deleted)"
    DELETION_STATUS["ecr"]="done"
fi

# =============================================================================
# STEP 10: DELETE CLOUDWATCH RESOURCES
# =============================================================================
log_step "STEP 10: Deleting CloudWatch Resources"

# Delete log groups
LOG_GROUPS=$(aws logs describe-log-groups --profile $AWS_PROFILE --log-group-name-prefix "/aws/emr-serverless/wikistream" --query 'logGroups[*].logGroupName' --output text 2>/dev/null || echo "")
if [ -n "$LOG_GROUPS" ]; then
    for LG in $LOG_GROUPS; do
        log_info "Deleting log group: $LG"
        aws logs delete-log-group --profile $AWS_PROFILE --log-group-name "$LG" 2>/dev/null || true
    done
fi

# Delete Lambda log groups
LAMBDA_LOGS=$(aws logs describe-log-groups --profile $AWS_PROFILE --log-group-name-prefix "/aws/lambda/wikistream" --query 'logGroups[*].logGroupName' --output text 2>/dev/null || echo "")
if [ -n "$LAMBDA_LOGS" ]; then
    for LG in $LAMBDA_LOGS; do
        log_info "Deleting log group: $LG"
        aws logs delete-log-group --profile $AWS_PROFILE --log-group-name "$LG" 2>/dev/null || true
    done
fi

# Delete dashboard
aws cloudwatch delete-dashboards --profile $AWS_PROFILE --dashboard-names "${NAME_PREFIX}-pipeline-dashboard" 2>/dev/null && \
    log_info "Deleted CloudWatch dashboard" || true

# Delete alarms
ALARMS=$(aws cloudwatch describe-alarms --profile $AWS_PROFILE --alarm-name-prefix "wikistream" --query 'MetricAlarms[*].AlarmName' --output text 2>/dev/null || echo "")
if [ -n "$ALARMS" ]; then
    aws cloudwatch delete-alarms --profile $AWS_PROFILE --alarm-names $ALARMS 2>/dev/null || true
    log_info "Deleted CloudWatch alarms"
fi

log_success "CloudWatch resources deleted"
DELETION_STATUS["cloudwatch"]="done"

# =============================================================================
# STEP 11: CLEANUP REMAINING RESOURCES (Fallback)
# =============================================================================
log_step "STEP 11: Cleaning Up Remaining Resources"

# Delete EMR applications
EMR_APPS=$(aws emr-serverless list-applications --profile $AWS_PROFILE --query "applications[?contains(name,\`${NAME_PREFIX}\`)].id" --output text 2>/dev/null || echo "")
if [ -n "$EMR_APPS" ]; then
    for APP_ID in $EMR_APPS; do
        log_info "Deleting EMR app: $APP_ID"
        aws emr-serverless delete-application --profile $AWS_PROFILE --application-id "$APP_ID" 2>/dev/null || true
    done
fi

# Delete MSK clusters
MSK_CLUSTERS=$(aws kafka list-clusters --profile $AWS_PROFILE --query "ClusterInfoList[?contains(ClusterName,\`${NAME_PREFIX}\`)].ClusterArn" --output text 2>/dev/null || echo "")
if [ -n "$MSK_CLUSTERS" ]; then
    for CLUSTER_ARN in $MSK_CLUSTERS; do
        log_info "Deleting MSK cluster: $(basename "$CLUSTER_ARN")"
        aws kafka delete-cluster --profile $AWS_PROFILE --cluster-arn "$CLUSTER_ARN" 2>/dev/null || true
    done
fi

# Delete ECS clusters (re-fetch to get current state)
ECS_CLUSTERS=$(aws ecs list-clusters --profile $AWS_PROFILE --query "clusterArns[?contains(@,\`${NAME_PREFIX}\`)]" --output text 2>/dev/null || echo "")
if [ -n "$ECS_CLUSTERS" ]; then
    for CLUSTER_ARN in $ECS_CLUSTERS; do
        CLUSTER_NAME=$(basename "$CLUSTER_ARN")
        # Force delete services
        SERVICES=$(aws ecs list-services --profile $AWS_PROFILE --cluster "$CLUSTER_NAME" --query 'serviceArns' --output text 2>/dev/null || echo "")
        if [ -n "$SERVICES" ]; then
            for SVC in $SERVICES; do
                aws ecs delete-service --profile $AWS_PROFILE --cluster "$CLUSTER_NAME" --service "$(basename "$SVC")" --force --no-cli-pager 2>/dev/null || true
            done
        fi
        aws ecs delete-cluster --profile $AWS_PROFILE --cluster "$CLUSTER_NAME" --no-cli-pager 2>/dev/null || true
    done
fi

# Delete Step Functions
SFN_ARNS=$(aws stepfunctions list-state-machines --profile $AWS_PROFILE --query "stateMachines[?contains(name,\`${NAME_PREFIX}\`)].stateMachineArn" --output text 2>/dev/null || echo "")
if [ -n "$SFN_ARNS" ]; then
    for SFN_ARN in $SFN_ARNS; do
        log_info "Deleting state machine: $(basename "$SFN_ARN")"
        aws stepfunctions delete-state-machine --profile $AWS_PROFILE --state-machine-arn "$SFN_ARN" 2>/dev/null || true
    done
fi

# Delete EventBridge rules
RULES=$(aws events list-rules --profile $AWS_PROFILE --name-prefix "wikistream" --query 'Rules[*].Name' --output text 2>/dev/null || echo "")
if [ -n "$RULES" ]; then
    for RULE in $RULES; do
        # Remove targets first
        TARGETS=$(aws events list-targets-by-rule --profile $AWS_PROFILE --rule "$RULE" --query 'Targets[*].Id' --output text 2>/dev/null || echo "")
        if [ -n "$TARGETS" ]; then
            for TARGET in $TARGETS; do
                aws events remove-targets --profile $AWS_PROFILE --rule "$RULE" --ids "$TARGET" 2>/dev/null || true
            done
        fi
        aws events delete-rule --profile $AWS_PROFILE --name "$RULE" 2>/dev/null || true
    done
fi

# Delete any remaining EventBridge Scheduler schedules
SCHEDULES=$(aws scheduler list-schedules --profile $AWS_PROFILE --name-prefix "wikistream" --query 'Schedules[*].Name' --output text 2>/dev/null || echo "")
if [ -n "$SCHEDULES" ]; then
    for SCHEDULE in $SCHEDULES; do
        log_info "Deleting schedule: $SCHEDULE"
        aws scheduler delete-schedule --profile $AWS_PROFILE --name "$SCHEDULE" 2>/dev/null || true
    done
fi

# Delete Lambda functions
LAMBDAS=$(aws lambda list-functions --profile $AWS_PROFILE --query 'Functions[?contains(FunctionName,`wikistream`)].FunctionName' --output text 2>/dev/null || echo "")
if [ -n "$LAMBDAS" ]; then
    for FUNC in $LAMBDAS; do
        log_info "Deleting Lambda: $FUNC"
        aws lambda delete-function --profile $AWS_PROFILE --function-name "$FUNC" 2>/dev/null || true
    done
fi

# Delete SNS topics
SNS_TOPICS=$(aws sns list-topics --profile $AWS_PROFILE --query "Topics[?contains(TopicArn,\`${NAME_PREFIX}\`)].TopicArn" --output text 2>/dev/null || echo "")
if [ -n "$SNS_TOPICS" ]; then
    for TOPIC in $SNS_TOPICS; do
        log_info "Deleting SNS topic: $(basename "$TOPIC")"
        aws sns delete-topic --profile $AWS_PROFILE --topic-arn "$TOPIC" 2>/dev/null || true
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
# STEP 13: FINAL VERIFICATION
# =============================================================================
log_step "STEP 13: Final Verification"

VERIFY_ERRORS=0

# Verify EMR applications deleted
EMR_CHECK=$(aws emr-serverless list-applications --profile $AWS_PROFILE --query 'applications[?contains(name,`wikistream`)].id' --output text 2>/dev/null || echo "")
if [ -n "$EMR_CHECK" ]; then
    log_warning "EMR applications still exist (may be terminating): $EMR_CHECK"
    VERIFY_ERRORS=$((VERIFY_ERRORS + 1))
else
    log_success "✓ EMR applications: deleted"
fi

# Verify S3 Tables bucket deleted
TABLE_BUCKET_CHECK=$(aws s3tables get-table-bucket --profile $AWS_PROFILE --table-bucket-arn "$TABLE_BUCKET_ARN" 2>&1 || echo "NOT_FOUND")
if [[ "$TABLE_BUCKET_CHECK" == *"NOT_FOUND"* ]] || [[ "$TABLE_BUCKET_CHECK" == *"ResourceNotFoundException"* ]] || [[ "$TABLE_BUCKET_CHECK" == *"NoSuchBucket"* ]]; then
    log_success "✓ S3 Tables bucket: deleted"
else
    log_warning "S3 Tables bucket may still be deleting (async operation)"
    VERIFY_ERRORS=$((VERIFY_ERRORS + 1))
fi

# Verify S3 data bucket deleted
if ! aws s3api head-bucket --profile $AWS_PROFILE --bucket "$DATA_BUCKET" --no-cli-pager 2>/dev/null; then
    log_success "✓ S3 data bucket: deleted"
else
    log_warning "S3 data bucket still exists"
    VERIFY_ERRORS=$((VERIFY_ERRORS + 1))
fi

# Verify Step Functions deleted
SFN_CHECK=$(aws stepfunctions list-state-machines --profile $AWS_PROFILE --query "stateMachines[?contains(name,\`${NAME_PREFIX}\`)].name" --output text 2>/dev/null || echo "")
if [ -z "$SFN_CHECK" ]; then
    log_success "✓ Step Functions: deleted"
else
    log_warning "Step Functions still exist: $SFN_CHECK"
    VERIFY_ERRORS=$((VERIFY_ERRORS + 1))
fi

# Verify ECR repository deleted
if ! aws ecr describe-repositories --profile $AWS_PROFILE --repository-names "${NAME_PREFIX}-producer" &>/dev/null; then
    log_success "✓ ECR repository: deleted"
else
    log_warning "ECR repository still exists"
    VERIFY_ERRORS=$((VERIFY_ERRORS + 1))
fi

# =============================================================================
# COMPLETE - SUMMARY
# =============================================================================
echo ""
echo "════════════════════════════════════════════════════════════════════"
if [ $VERIFY_ERRORS -eq 0 ]; then
    echo -e "${GREEN}✅ FULL DESTROY COMPLETED SUCCESSFULLY${NC}"
else
    echo -e "${YELLOW}⚠️  DESTROY COMPLETED WITH NOTES${NC}"
fi
echo "════════════════════════════════════════════════════════════════════"
echo ""
echo "📋 Deletion Summary:"
echo "   ├─ EventBridge:    ${DELETION_STATUS["eventbridge"]:-unknown}"
echo "   ├─ Step Functions: ${DELETION_STATUS["stepfunctions"]:-unknown}"
echo "   ├─ EMR Jobs:       ${DELETION_STATUS["emr_jobs"]:-unknown}"
echo "   ├─ EMR App:        ${DELETION_STATUS["emr_app"]:-unknown}"
echo "   ├─ ECS:            ${DELETION_STATUS["ecs"]:-unknown}"
echo "   ├─ Terraform:      ${DELETION_STATUS["terraform"]:-unknown}"
echo "   ├─ S3 Tables:      ${DELETION_STATUS["s3_tables"]:-unknown}"
echo "   ├─ S3 Bucket:      ${DELETION_STATUS["s3_bucket"]:-unknown}"
echo "   ├─ ECR:            ${DELETION_STATUS["ecr"]:-unknown}"
echo "   └─ CloudWatch:     ${DELETION_STATUS["cloudwatch"]:-unknown}"
echo ""
if [ $VERIFY_ERRORS -gt 0 ]; then
    echo -e "${YELLOW}⚠️  Note: $VERIFY_ERRORS resource(s) may still be terminating.${NC}"
    echo "   This is normal for MSK clusters and S3 Tables (can take 5-15 min)."
    echo "   They will complete deletion in the background."
    echo ""
fi
echo "🕐 Background deletions (no action needed):"
echo "   • MSK cluster: ~10-15 minutes to fully terminate"
echo "   • NAT Gateway: ~2-5 minutes"
echo "   • S3 Tables: ~5-10 minutes if async"
echo ""
echo "✅ Safe to run create_infra.sh immediately - Terraform"
echo "   will create new resources with fresh identifiers."
echo ""
echo "To recreate from scratch:"
echo "  ./scripts/create_infra.sh"
echo ""
