#!/bin/bash
# =============================================================================
# WikiStream - Destroy COSTLY Infrastructure Only (End of Day)
# =============================================================================
# Destroys expensive always-on resources while preserving data:
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
#   - Terraform state
# =============================================================================

set -e

echo "🔴 DESTROYING Costly WikiStream Infrastructure"
echo "==============================================="
echo ""
echo "Will DESTROY (saves ~\$19/day):"
echo "  ✗ NAT Gateway"
echo "  ✗ MSK Kafka Cluster"
echo "  ✗ EMR Serverless Application"
echo "  ✗ VPC and networking"
echo "  ✗ ECS Cluster"
echo ""
echo "Will PRESERVE (~\$1.30/day):"
echo "  ✓ S3 data bucket (bronze/silver/gold data)"
echo "  ✓ S3 Tables namespaces and tables"
echo "  ✓ ECR repository and Docker images"
echo ""

read -p "Proceed? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "❌ Cancelled."
    exit 1
fi

AWS_REGION="${AWS_REGION:-us-east-1}"

# Step 1: Stop EMR Serverless jobs and application
echo ""
echo "🛑 Step 1: Stopping EMR Serverless..."
EMR_APPS=$(aws emr-serverless list-applications --query 'applications[?contains(name,`wikistream`)].id' --output text 2>/dev/null || echo "")
for APP_ID in $EMR_APPS; do
    echo "   Cancelling jobs for: $APP_ID"
    RUNNING_JOBS=$(aws emr-serverless list-job-runs --application-id $APP_ID --states RUNNING PENDING SUBMITTED --query 'jobRuns[].id' --output text 2>/dev/null || echo "")
    for JOB_ID in $RUNNING_JOBS; do
        aws emr-serverless cancel-job-run --application-id $APP_ID --job-run-id $JOB_ID 2>/dev/null || true
    done
    echo "   Stopping application: $APP_ID"
    aws emr-serverless stop-application --application-id $APP_ID 2>/dev/null || true
done
echo "   ✅ EMR stopped"

# Step 2: Stop ECS service
echo ""
echo "📉 Step 2: Stopping ECS..."
ECS_CLUSTERS=$(aws ecs list-clusters --query 'clusterArns[?contains(@,`wikistream`)]' --output text 2>/dev/null || echo "")
for CLUSTER_ARN in $ECS_CLUSTERS; do
    CLUSTER_NAME=$(basename $CLUSTER_ARN)
    SERVICES=$(aws ecs list-services --cluster $CLUSTER_NAME --query 'serviceArns' --output text 2>/dev/null || echo "")
    for SVC in $SERVICES; do
        aws ecs update-service --cluster $CLUSTER_NAME --service $(basename $SVC) --desired-count 0 --no-cli-pager 2>/dev/null || true
    done
done
echo "   ✅ ECS stopped"

# Wait for EMR apps to stop
echo ""
echo "⏳ Waiting for EMR applications to stop..."
sleep 15

# Step 3: Delete EMR Serverless applications
echo ""
echo "🗑️  Step 3: Deleting EMR Serverless applications..."
for APP_ID in $EMR_APPS; do
    STATE=$(aws emr-serverless get-application --application-id $APP_ID --query 'application.state' --output text 2>/dev/null || echo "TERMINATED")
    if [ "$STATE" != "TERMINATED" ]; then
        aws emr-serverless delete-application --application-id $APP_ID 2>/dev/null || true
        echo "   Deleted: $APP_ID"
    fi
done
echo "   ✅ EMR deleted"

# Step 4: Delete MSK Clusters
echo ""
echo "🗑️  Step 4: Deleting MSK Clusters..."
MSK_CLUSTERS=$(aws kafka list-clusters --query 'ClusterInfoList[?contains(ClusterName,`wikistream`)].ClusterArn' --output text 2>/dev/null || echo "")
for CLUSTER_ARN in $MSK_CLUSTERS; do
    echo "   Deleting: $CLUSTER_ARN"
    aws kafka delete-cluster --cluster-arn $CLUSTER_ARN 2>/dev/null || true
done
[ -n "$MSK_CLUSTERS" ] && echo "   ✅ MSK deletion initiated (takes ~15-20 min)" || echo "   ✅ No MSK clusters found"

# Step 5: Delete NAT Gateways
echo ""
echo "🗑️  Step 5: Deleting NAT Gateways..."
NAT_GWS=$(aws ec2 describe-nat-gateways --filter "Name=state,Values=available,pending" --query 'NatGateways[].NatGatewayId' --output text 2>/dev/null || echo "")
for NAT_ID in $NAT_GWS; do
    echo "   Deleting: $NAT_ID"
    aws ec2 delete-nat-gateway --nat-gateway-id $NAT_ID 2>/dev/null || true
done
[ -n "$NAT_GWS" ] && echo "   ✅ NAT Gateway deletion initiated" || echo "   ✅ No NAT Gateways found"

# Wait for NAT Gateway to delete
if [ -n "$NAT_GWS" ]; then
    echo "   Waiting for NAT Gateway deletion..."
    sleep 60
fi

# Step 6: Release Elastic IPs
echo ""
echo "🗑️  Step 6: Releasing Elastic IPs..."
EIPS=$(aws ec2 describe-addresses --query 'Addresses[?AssociationId==`null`].AllocationId' --output text 2>/dev/null || echo "")
for EIP in $EIPS; do
    aws ec2 release-address --allocation-id $EIP 2>/dev/null || true
    echo "   Released: $EIP"
done
echo "   ✅ EIPs released"

# Step 7: Delete VPC resources
echo ""
echo "🗑️  Step 7: Cleaning up VPCs..."
VPCS=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=*wikistream*" --query 'Vpcs[].VpcId' --output text 2>/dev/null || echo "")
for VPC_ID in $VPCS; do
    echo "   Cleaning VPC: $VPC_ID"
    
    # Delete subnets
    for SUBNET in $(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --query 'Subnets[].SubnetId' --output text 2>/dev/null); do
        aws ec2 delete-subnet --subnet-id $SUBNET 2>/dev/null || true
    done
    
    # Delete route tables (except main)
    for RTB in $(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC_ID" --query 'RouteTables[?Associations[0].Main!=`true`].RouteTableId' --output text 2>/dev/null); do
        aws ec2 delete-route-table --route-table-id $RTB 2>/dev/null || true
    done
    
    # Detach and delete internet gateway
    for IGW in $(aws ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=$VPC_ID" --query 'InternetGateways[].InternetGatewayId' --output text 2>/dev/null); do
        aws ec2 detach-internet-gateway --internet-gateway-id $IGW --vpc-id $VPC_ID 2>/dev/null || true
        aws ec2 delete-internet-gateway --internet-gateway-id $IGW 2>/dev/null || true
    done
    
    # Delete security groups (except default)
    for SG in $(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID" --query 'SecurityGroups[?GroupName!=`default`].GroupId' --output text 2>/dev/null); do
        aws ec2 delete-security-group --group-id $SG 2>/dev/null || true
    done
    
    # Delete VPC
    aws ec2 delete-vpc --vpc-id $VPC_ID 2>/dev/null && echo "   ✅ VPC deleted: $VPC_ID" || echo "   ⚠️ VPC has dependencies: $VPC_ID"
done
[ -z "$VPCS" ] && echo "   ✅ No WikiStream VPCs found"

# Step 8: Delete ECS Clusters
echo ""
echo "🗑️  Step 8: Deleting ECS Clusters..."
for CLUSTER_ARN in $ECS_CLUSTERS; do
    CLUSTER_NAME=$(basename $CLUSTER_ARN)
    # Delete services first
    SERVICES=$(aws ecs list-services --cluster $CLUSTER_NAME --query 'serviceArns' --output text 2>/dev/null || echo "")
    for SVC in $SERVICES; do
        aws ecs delete-service --cluster $CLUSTER_NAME --service $(basename $SVC) --force --no-cli-pager 2>/dev/null || true
    done
    # Delete cluster
    aws ecs delete-cluster --cluster $CLUSTER_NAME --no-cli-pager 2>/dev/null || true
    echo "   Deleted: $CLUSTER_NAME"
done
echo "   ✅ ECS deleted"

echo ""
echo "========================================="
echo "✅ Costly infrastructure DESTROYED!"
echo "========================================="
echo ""
echo "💰 Estimated daily savings: ~\$19/day"
echo ""
echo "📦 Preserved resources:"
echo "   - S3: wikistream-dev-data-* (your data)"
echo "   - S3 Tables: bronze, silver, gold"
echo "   - ECR: wikistream-dev-producer (Docker image)"
echo ""
echo "To recreate tomorrow: ./scripts/create_infra.sh"
