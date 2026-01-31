#!/bin/bash
# =============================================================================
# List EMR Serverless Jobs
# =============================================================================
# Lists all EMR Serverless jobs with their current status
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
echo "📋 EMR Serverless Jobs Status"
echo "=============================="
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
# List All Jobs
# =============================================================================
log_info "Listing all jobs..."

ALL_JOBS=$(aws emr-serverless list-job-runs \
    --profile $AWS_PROFILE \
    --region $AWS_REGION \
    --application-id $EMR_APP_ID \
    --max-results 50 \
    --output json)

TOTAL_COUNT=$(echo "$ALL_JOBS" | jq '.jobRuns | length')

if [ "$TOTAL_COUNT" -eq 0 ]; then
    log_warning "No jobs found"
    exit 0
fi

echo ""
echo "📊 Job Summary"
echo "=============="
echo ""

# Count by state
RUNNING_COUNT=$(echo "$ALL_JOBS" | jq '[.jobRuns[] | select(.state=="RUNNING")] | length')
PENDING_COUNT=$(echo "$ALL_JOBS" | jq '[.jobRuns[] | select(.state=="PENDING" or .state=="SUBMITTED")] | length')
SUCCESS_COUNT=$(echo "$ALL_JOBS" | jq '[.jobRuns[] | select(.state=="SUCCESS" or .state=="COMPLETED")] | length')
FAILED_COUNT=$(echo "$ALL_JOBS" | jq '[.jobRuns[] | select(.state=="FAILED")] | length')
CANCELLED_COUNT=$(echo "$ALL_JOBS" | jq '[.jobRuns[] | select(.state=="CANCELLED")] | length')

printf "%-20s %s\n" "Running/Active:" "${RUNNING_COUNT}"
printf "%-20s %s\n" "Pending/Submitted:" "${PENDING_COUNT}"
printf "%-20s %s\n" "Succeeded:" "${SUCCESS_COUNT}"
printf "%-20s %s\n" "Failed:" "${FAILED_COUNT}"
printf "%-20s %s\n" "Cancelled:" "${CANCELLED_COUNT}"
printf "%-20s %s\n" "Total:" "${TOTAL_COUNT}"
echo ""

# =============================================================================
# Show Running Jobs
# =============================================================================
if [ "$RUNNING_COUNT" -gt 0 ] || [ "$PENDING_COUNT" -gt 0 ]; then
    echo "🟢 Running/Pending Jobs"
    echo "======================="
    echo ""
    echo "$ALL_JOBS" | jq -r '.jobRuns[] | select(.state=="RUNNING" or .state=="PENDING" or .state=="SUBMITTED") | "  • \(.name)\n    State: \(.state)\n    Job ID: \(.id)\n    Started: \(.createdAt)\n    Duration: \(if .endedAt == null then "Running..." else (.endedAt | fromdateiso8601 - .createdAt | fromdateiso8601 | tostring + " seconds") end)\n"' | sed '/^$/d'
    echo ""
fi

# =============================================================================
# Show Recent Completed/Failed Jobs (last 5)
# =============================================================================
echo "📜 Recent Job History (Last 10)"
echo "================================"
echo ""

echo "$ALL_JOBS" | jq -r '.jobRuns[] | "\(.state) | \(.name) | \(.id) | \(.createdAt) | \(.endedAt // "N/A")"' | sort -t'|' -k4 -r | head -10 | while IFS='|' read -r STATE NAME ID CREATED ENDED; do
    STATE_ICON=""
    case $STATE in
        "RUNNING") STATE_ICON="🟢" ;;
        "PENDING"|"SUBMITTED") STATE_ICON="🟡" ;;
        "SUCCESS"|"COMPLETED") STATE_ICON="✅" ;;
        "FAILED") STATE_ICON="❌" ;;
        "CANCELLED") STATE_ICON="⚠️" ;;
        *) STATE_ICON="⚪" ;;
    esac
    
    echo "  ${STATE_ICON} ${NAME} (${STATE})"
    echo "     Job ID: ${ID}"
    echo "     Started: ${CREATED}"
    if [ "$ENDED" != "N/A" ]; then
        echo "     Ended: ${ENDED}"
    fi
    echo ""
done

echo "✅ List complete!"
echo ""
