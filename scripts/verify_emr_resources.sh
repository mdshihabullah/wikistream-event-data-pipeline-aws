#!/bin/bash
# =============================================================================
# Verify EMR Serverless Resource Configuration
# =============================================================================
# Checks current vCPU allocation and identifies resource bottlenecks
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
echo "🔍 EMR Serverless Resource Verification"
echo "========================================"
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

# =============================================================================
# Check Pre-warming Configuration
# =============================================================================
echo ""
log_info "Checking pre-warming configuration..."

PREWARM_CONFIG=$(aws emr-serverless get-application \
    --profile $AWS_PROFILE \
    --region $AWS_REGION \
    --application-id $EMR_APP_ID \
    --query 'application.initialCapacity' \
    --output json)

if [ "$PREWARM_CONFIG" != "null" ]; then
    echo "$PREWARM_CONFIG" | jq -r 'to_entries[] | "  \(.key): \(.value.workerCount) workers × \(.value.workerConfiguration.cpu) = \((.value.workerCount | tonumber) * (.value.workerConfiguration.cpu | rtrimstr(" vCPU") | tonumber)) vCPU"'
    
    # Calculate total pre-warmed vCPU
    PREWARM_VCPU=$(echo "$PREWARM_CONFIG" | jq '[to_entries[] | (.value.workerCount | tonumber) * (.value.workerConfiguration.cpu | rtrimstr(" vCPU") | tonumber)] | add')
    log_info "Total Pre-warmed: ${PREWARM_VCPU} vCPU"
else
    log_warning "No pre-warming configured (cold start)"
    PREWARM_VCPU=0
fi

# =============================================================================
# Check Maximum Capacity
# =============================================================================
echo ""
log_info "Checking maximum capacity limits..."

MAX_CAPACITY=$(aws emr-serverless get-application \
    --profile $AWS_PROFILE \
    --region $AWS_REGION \
    --application-id $EMR_APP_ID \
    --query 'application.maximumCapacity' \
    --output json)

MAX_VCPU=$(echo "$MAX_CAPACITY" | jq -r '.cpu | rtrimstr(" vCPU")')
MAX_MEMORY=$(echo "$MAX_CAPACITY" | jq -r '.memory')
MAX_DISK=$(echo "$MAX_CAPACITY" | jq -r '.disk')

log_success "Max Capacity: ${MAX_VCPU} vCPU, ${MAX_MEMORY} memory, ${MAX_DISK} disk"

# =============================================================================
# Check Running Jobs
# =============================================================================
echo ""
log_info "Checking running jobs..."

RUNNING_JOBS=$(aws emr-serverless list-job-runs \
    --profile $AWS_PROFILE \
    --region $AWS_REGION \
    --application-id $EMR_APP_ID \
    --states RUNNING PENDING SUBMITTED \
    --max-results 20 \
    --output json)

JOB_COUNT=$(echo "$RUNNING_JOBS" | jq '.jobRuns | length')

if [ "$JOB_COUNT" -eq 0 ]; then
    log_warning "No jobs currently running"
    RUNNING_VCPU=0
else
    log_success "Found ${JOB_COUNT} running/pending job(s):"
    echo "$RUNNING_JOBS" | jq -r '.jobRuns[] | "  • \(.name) (\(.state)) - Started: \(.createdAt)"'
    
    # Estimate vCPU usage (simplified)
    # Bronze streaming: 3 vCPU (1 driver + 1 executor)
    # Batch jobs: 5 vCPU (1 driver + 2 executors)
    BRONZE_JOBS=$(echo "$RUNNING_JOBS" | jq '[.jobRuns[] | select(.name | contains("bronze"))] | length')
    BATCH_JOBS=$(echo "$RUNNING_JOBS" | jq '[.jobRuns[] | select(.name | contains("bronze") | not)] | length')
    
    RUNNING_VCPU=$((BRONZE_JOBS * 3 + BATCH_JOBS * 5))
    log_info "Estimated running jobs vCPU: ${RUNNING_VCPU} vCPU"
fi

# =============================================================================
# Resource Summary
# =============================================================================
echo ""
echo "📊 Resource Summary"
echo "==================="
echo ""
printf "%-25s %s\n" "Max vCPU Quota:" "${MAX_VCPU} vCPU"
printf "%-25s %s\n" "Pre-warming:" "${PREWARM_VCPU} vCPU"
printf "%-25s %s\n" "Running Jobs:" "${RUNNING_VCPU} vCPU"
echo "─────────────────────────────────"
TOTAL_USED=$((PREWARM_VCPU + RUNNING_VCPU))
AVAILABLE=$((MAX_VCPU - TOTAL_USED))
printf "%-25s %s\n" "Total Used:" "${TOTAL_USED} vCPU"
printf "%-25s %s\n" "Available:" "${AVAILABLE} vCPU"
echo ""

# =============================================================================
# Health Check
# =============================================================================
echo "🏥 Health Check"
echo "==============="
echo ""

# Check if there's enough capacity for a batch job (5 vCPU)
if [ "$AVAILABLE" -ge 5 ]; then
    log_success "Sufficient capacity available for batch jobs (${AVAILABLE} vCPU available, 5 vCPU needed)"
elif [ "$AVAILABLE" -ge 3 ]; then
    log_warning "Tight capacity (${AVAILABLE} vCPU available). May have issues with concurrent jobs."
else
    log_error "INSUFFICIENT CAPACITY (${AVAILABLE} vCPU available, 5 vCPU needed for batch jobs)"
    echo ""
    echo "Recommendations:"
    echo "  1. Reduce pre-warming: emr_prewarm_executor_count = 0"
    echo "  2. Stop non-essential jobs"
    echo "  3. Reduce batch job executors: spark.executor.instances=1"
    echo "  4. Request AWS quota increase for vCPU"
fi

# =============================================================================
# Check for Recent Failures
# =============================================================================
echo ""
log_info "Checking for recent capacity-related failures..."

RECENT_FAILURES=$(aws emr-serverless list-job-runs \
    --profile $AWS_PROFILE \
    --region $AWS_REGION \
    --application-id $EMR_APP_ID \
    --states FAILED \
    --max-results 5 \
    --output json)

CAPACITY_FAILURES=$(echo "$RECENT_FAILURES" | jq '[.jobRuns[] | select(.stateDetails | contains("ApplicationMaxCapacityExceededException"))] | length')

if [ "$CAPACITY_FAILURES" -gt 0 ]; then
    log_error "Found ${CAPACITY_FAILURES} recent capacity-related failure(s)!"
    echo "$RECENT_FAILURES" | jq -r '.jobRuns[] | select(.stateDetails | contains("ApplicationMaxCapacityExceededException")) | "  • \(.name) - \(.stateDetails | split("\n")[0])"'
    echo ""
    log_error "ACTION REQUIRED: Reduce resource allocation (see EMR_RESOURCE_OPTIMIZATION.md)"
else
    log_success "No recent capacity-related failures"
fi

# =============================================================================
# Configuration Recommendations
# =============================================================================
echo ""
echo "💡 Configuration Recommendations"
echo "================================="
echo ""

if [ "$PREWARM_VCPU" -gt 4 ]; then
    log_warning "Pre-warming is high (${PREWARM_VCPU} vCPU). Consider reducing to 4 vCPU:"
    echo "  emr_prewarm_driver_count   = 1  (2 vCPU)"
    echo "  emr_prewarm_executor_count = 1  (2 vCPU)"
    echo ""
fi

if [ "$AVAILABLE" -lt 6 ]; then
    log_warning "Low buffer capacity (${AVAILABLE} vCPU). Consider:"
    echo "  • Reducing pre-warming to 0 vCPU (slower startup)"
    echo "  • Using 1 executor per batch job (slower processing)"
    echo "  • Requesting AWS vCPU quota increase"
    echo ""
fi

if [ "$MAX_VCPU" -le 16 ]; then
    log_info "Running on minimum vCPU quota (${MAX_VCPU} vCPU)."
    echo "  For production, consider requesting 32+ vCPU quota increase."
    echo ""
fi

echo "✅ Verification complete!"
echo ""
