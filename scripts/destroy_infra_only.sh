#!/bin/bash
# =============================================================================
# WikiStream - Destroy COSTLY Infrastructure Only (End of Day)
# =============================================================================
# Destroys expensive always-on resources while PRESERVING data:
# 
# DESTROYED (saves ~$19/day):
#   - NAT Gateway (~$12/day)
#   - MSK Kafka Cluster (~$3/day)
#   - EMR Serverless Application (~$3.51/day)
#   - VPC and networking
#   - ECS Cluster
#
# PRESERVED (costs ~$1.30/day):
#   - S3 data bucket (your bronze/silver/gold data)
#   - S3 Tables namespaces and tables
#   - ECR repository and Docker images
#   - Terraform state (for fast rebuild)
#
# Usage:
#   ./scripts/destroy_infra.sh          # Interactive confirmation
#   ./scripts/destroy_infra.sh --force  # Skip confirmation (CI/CD)
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

echo ""
echo "🔴 DESTROYING Costly WikiStream Infrastructure"
echo "==============================================="
echo ""
echo "Will DESTROY (saves ~\$19/day):"
echo "  ✗ NAT Gateway"
echo "  ✗ MSK Kafka Cluster"
echo "  ✗ EMR Serverless Application"
echo "  ✗ VPC and networking"
echo "  ✗ ECS Cluster"
echo "  ✗ Step Functions & EventBridge"
echo ""
echo "Will PRESERVE (~\$1.30/day):"
echo "  ✓ S3 data bucket (bronze/silver/gold data)"
echo "  ✓ S3 Tables namespaces and tables"
echo "  ✓ ECR repository and Docker images"
echo ""

if [ "$1" != "--force" ]; then
    read -p "Proceed? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        log_error "Cancelled. Type 'yes' to confirm."
        echo "Tip: Use --force flag to skip confirmation"
        exit 1
    fi
else
    log_warning "Running with --force flag, skipping confirmation"
fi

# =============================================================================
# STEP 1: DISABLE EVENTBRIDGE RULES
# =============================================================================
log_step "STEP 1: Disabling EventBridge Rules"

RULES=$(aws events list-rules --name-prefix "wikistream" --query 'Rules[*].Name' --output text 2>/dev/null || echo "")
for RULE in $RULES; do
    log_info "Disabling: $RULE"
    aws events disable-rule --name "$RULE" 2>/dev/null || true
done
log_success "EventBridge rules disabled"

# =============================================================================
# STEP 2: STOP STEP FUNCTIONS EXECUTIONS
# =============================================================================
log_step "STEP 2: Stopping Step Functions Executions"

SFN_ARNS=$(aws stepfunctions list-state-machines --query 'stateMachines[?contains(name,`wikistream`)].stateMachineArn' --output text 2>/dev/null || echo "")
for SFN_ARN in $SFN_ARNS; do
    RUNNING_EXECS=$(aws stepfunctions list-executions --state-machine-arn "$SFN_ARN" --status-filter RUNNING --query 'executions[*].executionArn' --output text 2>/dev/null || echo "")
    for EXEC_ARN in $RUNNING_EXECS; do
        log_info "Stopping: $(basename "$EXEC_ARN")"
        aws stepfunctions stop-execution --execution-arn "$EXEC_ARN" --cause "Infrastructure destroy" 2>/dev/null || true
    done
done
log_success "Step Functions stopped"

# =============================================================================
# STEP 3: CANCEL ALL EMR JOBS AND WAIT
# =============================================================================
log_step "STEP 3: Cancelling EMR Serverless Jobs"

EMR_APPS=$(aws emr-serverless list-applications --query 'applications[?contains(name,`wikistream`)].[id,name,state]' --output text 2>/dev/null || echo "")

if [ -n "$EMR_APPS" ]; then
    while IFS=$'\t' read -r APP_ID APP_NAME APP_STATE; do
        [ -z "$APP_ID" ] && continue
        
        log_info "Processing: $APP_NAME ($APP_ID)"
        
        # Cancel all jobs
        JOBS=$(aws emr-serverless list-job-runs --application-id "$APP_ID" --states RUNNING PENDING SUBMITTED SCHEDULED --query 'jobRuns[*].id' --output text 2>/dev/null || echo "")
        for JOB_ID in $JOBS; do
            log_info "  Cancelling job: $JOB_ID"
            aws emr-serverless cancel-job-run --application-id "$APP_ID" --job-run-id "$JOB_ID" 2>/dev/null || true
        done
        
        # Wait for jobs to cancel
        if [ -n "$JOBS" ]; then
            log_info "  Waiting for jobs to cancel..."
            for i in {1..24}; do
                STILL_RUNNING=$(aws emr-serverless list-job-runs --application-id "$APP_ID" --states RUNNING PENDING SUBMITTED CANCELLING --query 'jobRuns[*].id' --output text 2>/dev/null || echo "")
                [ -z "$STILL_RUNNING" ] && break
                echo -n "."
                sleep 5
            done
            echo ""
        fi
        
    done <<< "$EMR_APPS"
fi
log_success "EMR jobs cancelled"

# =============================================================================
# STEP 4: STOP EMR APPLICATIONS AND WAIT
# =============================================================================
log_step "STEP 4: Stopping EMR Applications"

EMR_APPS=$(aws emr-serverless list-applications --query 'applications[?contains(name,`wikistream`)].[id,name,state]' --output text 2>/dev/null || echo "")

if [ -n "$EMR_APPS" ]; then
    while IFS=$'\t' read -r APP_ID APP_NAME APP_STATE; do
        [ -z "$APP_ID" ] && continue
        
        if [ "$APP_STATE" == "STARTED" ] || [ "$APP_STATE" == "STARTING" ]; then
            log_info "Stopping: $APP_NAME"
            aws emr-serverless stop-application --application-id "$APP_ID" 2>/dev/null || true
            
            # Wait for STOPPED
            for i in {1..24}; do
                STATE=$(aws emr-serverless get-application --application-id "$APP_ID" --query 'application.state' --output text 2>/dev/null || echo "STOPPED")
                [ "$STATE" == "STOPPED" ] || [ "$STATE" == "TERMINATED" ] && break
                echo -n "."
                sleep 5
            done
            echo ""
        fi
    done <<< "$EMR_APPS"
fi
log_success "EMR applications stopped"

# =============================================================================
# STEP 5: STOP ECS SERVICES
# =============================================================================
log_step "STEP 5: Stopping ECS Services"

ECS_CLUSTERS=$(aws ecs list-clusters --query 'clusterArns[?contains(@,`wikistream`)]' --output text 2>/dev/null || echo "")
for CLUSTER_ARN in $ECS_CLUSTERS; do
    CLUSTER_NAME=$(basename "$CLUSTER_ARN")
    SERVICES=$(aws ecs list-services --cluster "$CLUSTER_NAME" --query 'serviceArns' --output text 2>/dev/null || echo "")
    for SVC in $SERVICES; do
        log_info "Stopping: $(basename "$SVC")"
        aws ecs update-service --cluster "$CLUSTER_NAME" --service "$(basename "$SVC")" --desired-count 0 --no-cli-pager 2>/dev/null || true
    done
done
log_success "ECS services stopped"

# =============================================================================
# STEP 6: DELETE EMR APPLICATIONS
# =============================================================================
log_step "STEP 6: Deleting EMR Applications"

sleep 5  # Give time for state to settle
EMR_APPS=$(aws emr-serverless list-applications --query 'applications[?contains(name,`wikistream`)].id' --output text 2>/dev/null || echo "")
for APP_ID in $EMR_APPS; do
    log_info "Deleting: $APP_ID"
    aws emr-serverless delete-application --application-id "$APP_ID" 2>/dev/null || true
done
log_success "EMR applications deleted"

# =============================================================================
# STEP 7: DELETE MSK CLUSTERS
# =============================================================================
log_step "STEP 7: Deleting MSK Clusters"

MSK_CLUSTERS=$(aws kafka list-clusters --query 'ClusterInfoList[?contains(ClusterName,`wikistream`)].ClusterArn' --output text 2>/dev/null || echo "")
for CLUSTER_ARN in $MSK_CLUSTERS; do
    log_info "Deleting: $(basename "$CLUSTER_ARN")"
    aws kafka delete-cluster --cluster-arn "$CLUSTER_ARN" 2>/dev/null || true
done
[ -n "$MSK_CLUSTERS" ] && log_warning "MSK deletion takes ~15-20 min in background" || log_info "No MSK clusters"
log_success "MSK deletion initiated"

# =============================================================================
# STEP 8: DELETE NAT GATEWAYS
# =============================================================================
log_step "STEP 8: Deleting NAT Gateways"

NAT_GWS=$(aws ec2 describe-nat-gateways --filter "Name=state,Values=available,pending" --query 'NatGateways[].NatGatewayId' --output text 2>/dev/null || echo "")
for NAT_ID in $NAT_GWS; do
    log_info "Deleting: $NAT_ID"
    aws ec2 delete-nat-gateway --nat-gateway-id "$NAT_ID" 2>/dev/null || true
done

if [ -n "$NAT_GWS" ]; then
    log_info "Waiting for NAT Gateway deletion (60s)..."
    sleep 60
fi
log_success "NAT Gateways deleted"

# =============================================================================
# STEP 9: RELEASE ELASTIC IPS
# =============================================================================
log_step "STEP 9: Releasing Elastic IPs"

EIPS=$(aws ec2 describe-addresses --query 'Addresses[?AssociationId==`null`].AllocationId' --output text 2>/dev/null || echo "")
for EIP in $EIPS; do
    log_info "Releasing: $EIP"
    aws ec2 release-address --allocation-id "$EIP" 2>/dev/null || true
done
log_success "EIPs released"

# =============================================================================
# STEP 10: DELETE VPC RESOURCES
# =============================================================================
log_step "STEP 10: Cleaning Up VPCs"

VPCS=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=*wikistream*" --query 'Vpcs[].VpcId' --output text 2>/dev/null || echo "")
for VPC_ID in $VPCS; do
    log_info "Cleaning VPC: $VPC_ID"
    
    # Delete subnets
    for SUBNET in $(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --query 'Subnets[].SubnetId' --output text 2>/dev/null); do
        aws ec2 delete-subnet --subnet-id "$SUBNET" 2>/dev/null || true
    done
    
    # Delete route tables (except main)
    for RTB in $(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC_ID" --query 'RouteTables[?Associations[0].Main!=`true`].RouteTableId' --output text 2>/dev/null); do
        aws ec2 delete-route-table --route-table-id "$RTB" 2>/dev/null || true
    done
    
    # Detach and delete IGW
    for IGW in $(aws ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=$VPC_ID" --query 'InternetGateways[].InternetGatewayId' --output text 2>/dev/null); do
        aws ec2 detach-internet-gateway --internet-gateway-id "$IGW" --vpc-id "$VPC_ID" 2>/dev/null || true
        aws ec2 delete-internet-gateway --internet-gateway-id "$IGW" 2>/dev/null || true
    done
    
    # Delete security groups (except default)
    for SG in $(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID" --query 'SecurityGroups[?GroupName!=`default`].GroupId' --output text 2>/dev/null); do
        aws ec2 delete-security-group --group-id "$SG" 2>/dev/null || true
    done
    
    # Delete VPC
    aws ec2 delete-vpc --vpc-id "$VPC_ID" 2>/dev/null && log_success "VPC deleted" || log_warning "VPC has dependencies"
done
[ -z "$VPCS" ] && log_info "No WikiStream VPCs found"

# =============================================================================
# STEP 11: DELETE ECS CLUSTERS
# =============================================================================
log_step "STEP 11: Deleting ECS Clusters"

for CLUSTER_ARN in $ECS_CLUSTERS; do
    CLUSTER_NAME=$(basename "$CLUSTER_ARN")
    SERVICES=$(aws ecs list-services --cluster "$CLUSTER_NAME" --query 'serviceArns' --output text 2>/dev/null || echo "")
    for SVC in $SERVICES; do
        aws ecs delete-service --cluster "$CLUSTER_NAME" --service "$(basename "$SVC")" --force --no-cli-pager 2>/dev/null || true
    done
    aws ecs delete-cluster --cluster "$CLUSTER_NAME" --no-cli-pager 2>/dev/null || true
    log_info "Deleted: $CLUSTER_NAME"
done
log_success "ECS clusters deleted"

# =============================================================================
# STEP 12: DELETE STEP FUNCTIONS & EVENTBRIDGE
# =============================================================================
log_step "STEP 12: Cleaning Step Functions & EventBridge"

for SFN_ARN in $SFN_ARNS; do
    log_info "Deleting: $(basename "$SFN_ARN")"
    aws stepfunctions delete-state-machine --state-machine-arn "$SFN_ARN" 2>/dev/null || true
done

for RULE in $RULES; do
    TARGETS=$(aws events list-targets-by-rule --rule "$RULE" --query 'Targets[*].Id' --output text 2>/dev/null || echo "")
    for TARGET in $TARGETS; do
        aws events remove-targets --rule "$RULE" --ids "$TARGET" 2>/dev/null || true
    done
    aws events delete-rule --name "$RULE" 2>/dev/null || true
done
log_success "Step Functions & EventBridge cleaned"

# =============================================================================
# COMPLETE
# =============================================================================
echo ""
echo "========================================="
echo -e "${GREEN}✅ Costly Infrastructure DESTROYED!${NC}"
echo "========================================="
echo ""
echo "💰 Estimated daily savings: ~\$19/day"
echo ""
echo "📦 Preserved resources:"
echo "   - S3: wikistream-dev-data-* (your data)"
echo "   - S3 Tables: bronze, silver, gold, dq_audit"
echo "   - ECR: wikistream-dev-producer (Docker image)"
echo ""
echo "Note: MSK deletion takes ~15-20 min in background"
echo ""
echo "To recreate: ./scripts/create_infra.sh"
