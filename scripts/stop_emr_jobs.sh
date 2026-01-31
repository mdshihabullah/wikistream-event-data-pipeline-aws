#!/bin/bash
# =============================================================================
# Stop EMR Serverless Jobs
# =============================================================================
# Stops all running EMR Serverless jobs including bronze streaming and batch jobs
# =============================================================================

set -e

AWS_PROFILE="${AWS_PROFILE:-neuefische}"
AWS_REGION="${AWS_REGION:-us-east-1}"
ENVIRONMENT="${1:-dev}"
NAME_PREFIX="wikistream-${ENVIRONMENT}"

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
echo "🛑 Stopping EMR Serverless Jobs"
echo "================================"
echo ""

# =============================================================================
# Get EMR Serverless Application ID
# =============================================================================
log_info "Getting EMR Serverless Application ID..."
EMR_APP_ID=$(aws emr-serverless list-applications \
    --profile $AWS_PROFILE \
    --region $AWS_REGION \
    --query "applications[?name=='${NAME_PREFIX}-spark'].id" \
    --output text)

if [ -z "$EMR_APP_ID" ]; then
    log_error "EMR Serverless application not found"
    exit 1
fi

log_success "EMR App ID: ${EMR_APP_ID}"
echo ""

# =============================================================================
# List and Cancel Running Jobs
# =============================================================================
log_info "Listing running and pending jobs..."

RUNNING_JOBS=$(aws emr-serverless list-job-runs \
    --profile $AWS_PROFILE \
    --region $AWS_REGION \
    --application-id $EMR_APP_ID \
    --states RUNNING PENDING SUBMITTED \
    --max-results 50 \
    --output json)

JOB_COUNT=$(echo "$RUNNING_JOBS" | jq '.jobRuns | length')

if [ "$JOB_COUNT" -eq 0 ]; then
    log_success "No jobs currently running"
    exit 0
fi

log_warning "Found ${JOB_COUNT} running/pending job(s):"
echo ""

# Display jobs and stop them
echo "$RUNNING_JOBS" | jq -r '.jobRuns[] | "  • \(.name) (\(.state)) - Job ID: \(.id) - Started: \(.createdAt)"'
echo ""

log_info "Stopping jobs..."
echo ""

# Cancel each job
for JOB_ID in $(echo "$RUNNING_JOBS" | jq -r '.jobRuns[].id'); do
    JOB_NAME=$(echo "$RUNNING_JOBS" | jq -r ".jobRuns[] | select(.id==\"${JOB_ID}\") | .name")
    JOB_STATE=$(echo "$RUNNING_JOBS" | jq -r ".jobRuns[] | select(.id==\"${JOB_ID}\") | .state")
    
    log_info "Cancelling job: ${JOB_NAME} (${JOB_STATE})"
    
    aws emr-serverless cancel-job-run \
        --profile $AWS_PROFILE \
        --region $AWS_REGION \
        --application-id $EMR_APP_ID \
        --job-run-id $JOB_ID \
        > /dev/null 2>&1
    
    log_success "Cancelled: ${JOB_NAME} (${JOB_ID})"
done

# =============================================================================
# Verify Jobs Are Stopped
# =============================================================================
echo ""
log_info "Verifying jobs are stopped..."

sleep 3

RUNNING_AFTER=$(aws emr-serverless list-job-runs \
    --profile $AWS_PROFILE \
    --region $AWS_REGION \
    --application-id $EMR_APP_ID \
    --states RUNNING PENDING SUBMITTED \
    --max-results 50 \
    --output json)

REMAINING_COUNT=$(echo "$RUNNING_AFTER" | jq '.jobRuns | length')

if [ "$REMAINING_COUNT" -eq 0 ]; then
    log_success "All jobs successfully stopped"
else
    log_warning "${REMAINING_COUNT} job(s) still in running/pending state (may take a few moments to fully stop)"
fi

echo ""
log_info "Job cancellation initiated. Use './scripts/verify_emr_resources.sh' to verify status"
echo ""

# =============================================================================
# Optional: Stop EMR Application (if --stop-app flag is provided)
# =============================================================================
if [ "$2" == "--stop-app" ]; then
    log_warning "Stopping EMR Serverless application..."
    aws emr-serverless stop-application \
        --profile $AWS_PROFILE \
        --region $AWS_REGION \
        --application-id $EMR_APP_ID > /dev/null 2>&1
    
    log_success "EMR Serverless application stopped"
    echo ""
    log_warning "Note: Application auto-start is enabled. It will restart when a new job is submitted."
    echo ""
fi

echo "✅ Stop operation complete!"
echo ""
