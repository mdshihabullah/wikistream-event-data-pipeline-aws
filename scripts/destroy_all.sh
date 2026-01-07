#!/bin/bash
# =============================================================================
# WikiStream - FULL DESTROY (Emergency/Complete Teardown)
# =============================================================================
# ⚠️  WARNING: This destroys EVERYTHING including your data!
# Use this only when you want to completely remove all resources.
#
# For daily dev workflow, use destroy_infra.sh instead (preserves data).
# =============================================================================

set -e

echo "🚨 FULL DESTROY - WikiStream Infrastructure"
echo "============================================="
echo ""
echo "⚠️  WARNING: This will DELETE EVERYTHING including:"
echo "  ✗ All S3 Tables data (bronze, silver, gold)"
echo "  ✗ All S3 bucket data"
echo "  ✗ ECR repository and Docker images"
echo "  ✗ All compute resources"
echo "  ✗ All networking"
echo ""
echo "This action is IRREVERSIBLE!"
echo ""

read -p "Type 'DELETE EVERYTHING' to confirm: " confirm
if [ "$confirm" != "DELETE EVERYTHING" ]; then
    echo "❌ Cancelled. Use './scripts/destroy_infra.sh' for partial destroy."
    exit 1
fi

AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo ""
echo "🛑 Step 1: Stopping all running services..."

# Stop EMR jobs
for APP_ID in $(aws emr-serverless list-applications --query 'applications[?contains(name,`wikistream`)].id' --output text 2>/dev/null); do
    for JOB_ID in $(aws emr-serverless list-job-runs --application-id $APP_ID --states RUNNING PENDING SUBMITTED --query 'jobRuns[].id' --output text 2>/dev/null); do
        aws emr-serverless cancel-job-run --application-id $APP_ID --job-run-id $JOB_ID 2>/dev/null || true
    done
    aws emr-serverless stop-application --application-id $APP_ID 2>/dev/null || true
done

# Stop ECS
for CLUSTER in $(aws ecs list-clusters --query 'clusterArns[?contains(@,`wikistream`)]' --output text 2>/dev/null); do
    CLUSTER_NAME=$(basename $CLUSTER)
    for SVC in $(aws ecs list-services --cluster $CLUSTER_NAME --query 'serviceArns' --output text 2>/dev/null); do
        aws ecs update-service --cluster $CLUSTER_NAME --service $(basename $SVC) --desired-count 0 --no-cli-pager 2>/dev/null || true
    done
done
echo "   ✅ Services stopped"

echo ""
echo "🗑️  Step 2: Deleting S3 Tables..."
TABLE_BUCKET_ARN="arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/wikistream-dev-tables"

# Delete tables in each namespace (including dq_audit)
for NS in bronze silver gold dq_audit; do
    TABLES=$(aws s3tables list-tables --table-bucket-arn $TABLE_BUCKET_ARN --namespace $NS --query 'tables[*].name' --output text 2>/dev/null || echo "")
    for TABLE in $TABLES; do
        echo "   Deleting ${NS}.${TABLE}..."
        aws s3tables delete-table --table-bucket-arn $TABLE_BUCKET_ARN --namespace $NS --name $TABLE 2>/dev/null || true
    done
done

# Delete namespaces (including dq_audit)
for NS in bronze silver gold dq_audit; do
    aws s3tables delete-namespace --table-bucket-arn $TABLE_BUCKET_ARN --namespace $NS 2>/dev/null || true
done

# Delete table bucket
aws s3tables delete-table-bucket --table-bucket-arn $TABLE_BUCKET_ARN 2>/dev/null || true
echo "   ✅ S3 Tables deleted"

echo ""
echo "🗑️  Step 3: Emptying and deleting S3 bucket..."
DATA_BUCKET="wikistream-dev-data-${AWS_ACCOUNT_ID}"

# Empty bucket (including versions)
aws s3 rm s3://${DATA_BUCKET} --recursive 2>/dev/null || true

# Delete all versions
aws s3api list-object-versions --bucket ${DATA_BUCKET} --query 'Versions[].{Key:Key,VersionId:VersionId}' --output json 2>/dev/null | \
    jq -c '.[] | select(. != null)' 2>/dev/null | while read obj; do
        KEY=$(echo $obj | jq -r '.Key')
        VERSION=$(echo $obj | jq -r '.VersionId')
        aws s3api delete-object --bucket ${DATA_BUCKET} --key "${KEY}" --version-id "${VERSION}" 2>/dev/null
    done

# Delete delete markers
aws s3api list-object-versions --bucket ${DATA_BUCKET} --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' --output json 2>/dev/null | \
    jq -c '.[] | select(. != null)' 2>/dev/null | while read obj; do
        KEY=$(echo $obj | jq -r '.Key')
        VERSION=$(echo $obj | jq -r '.VersionId')
        aws s3api delete-object --bucket ${DATA_BUCKET} --key "${KEY}" --version-id "${VERSION}" 2>/dev/null
    done

aws s3api delete-bucket --bucket ${DATA_BUCKET} 2>/dev/null || true
echo "   ✅ S3 bucket deleted"

echo ""
echo "🗑️  Step 4: Deleting ECR repository..."
aws ecr delete-repository --repository-name wikistream-dev-producer --force 2>/dev/null || true
echo "   ✅ ECR deleted"

echo ""
echo "🗑️  Step 5: Deleting remaining infrastructure via AWS CLI..."

# Delete EMR applications
sleep 10
for APP_ID in $(aws emr-serverless list-applications --query 'applications[?contains(name,`wikistream`)].id' --output text 2>/dev/null); do
    aws emr-serverless delete-application --application-id $APP_ID 2>/dev/null || true
done

# Delete MSK clusters
for CLUSTER_ARN in $(aws kafka list-clusters --query 'ClusterInfoList[?contains(ClusterName,`wikistream`)].ClusterArn' --output text 2>/dev/null); do
    aws kafka delete-cluster --cluster-arn $CLUSTER_ARN 2>/dev/null || true
done

# Delete NAT Gateways
for NAT_ID in $(aws ec2 describe-nat-gateways --filter "Name=state,Values=available,pending" --query 'NatGateways[].NatGatewayId' --output text 2>/dev/null); do
    aws ec2 delete-nat-gateway --nat-gateway-id $NAT_ID 2>/dev/null || true
done

sleep 60  # Wait for NAT to delete

# Release EIPs
for EIP in $(aws ec2 describe-addresses --query 'Addresses[?AssociationId==`null`].AllocationId' --output text 2>/dev/null); do
    aws ec2 release-address --allocation-id $EIP 2>/dev/null || true
done

# Delete VPCs
for VPC_ID in $(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=*wikistream*" --query 'Vpcs[].VpcId' --output text 2>/dev/null); do
    # Delete subnets
    for SUBNET in $(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --query 'Subnets[].SubnetId' --output text 2>/dev/null); do
        aws ec2 delete-subnet --subnet-id $SUBNET 2>/dev/null || true
    done
    # Delete route tables
    for RTB in $(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC_ID" --query 'RouteTables[?Associations[0].Main!=`true`].RouteTableId' --output text 2>/dev/null); do
        aws ec2 delete-route-table --route-table-id $RTB 2>/dev/null || true
    done
    # Delete IGW
    for IGW in $(aws ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=$VPC_ID" --query 'InternetGateways[].InternetGatewayId' --output text 2>/dev/null); do
        aws ec2 detach-internet-gateway --internet-gateway-id $IGW --vpc-id $VPC_ID 2>/dev/null || true
        aws ec2 delete-internet-gateway --internet-gateway-id $IGW 2>/dev/null || true
    done
    # Delete security groups
    for SG in $(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID" --query 'SecurityGroups[?GroupName!=`default`].GroupId' --output text 2>/dev/null); do
        aws ec2 delete-security-group --group-id $SG 2>/dev/null || true
    done
    # Delete VPC
    aws ec2 delete-vpc --vpc-id $VPC_ID 2>/dev/null || true
done

# Delete ECS clusters
for CLUSTER in $(aws ecs list-clusters --query 'clusterArns[?contains(@,`wikistream`)]' --output text 2>/dev/null); do
    CLUSTER_NAME=$(basename $CLUSTER)
    for SVC in $(aws ecs list-services --cluster $CLUSTER_NAME --query 'serviceArns' --output text 2>/dev/null); do
        aws ecs delete-service --cluster $CLUSTER_NAME --service $(basename $SVC) --force --no-cli-pager 2>/dev/null || true
    done
    aws ecs delete-cluster --cluster $CLUSTER_NAME --no-cli-pager 2>/dev/null || true
done

echo "   ✅ Infrastructure deleted"

echo ""
echo "========================================"
echo "✅ FULL DESTROY COMPLETED"
echo "========================================"
echo ""
echo "All WikiStream resources have been deleted."
echo ""
echo "To recreate from scratch: ./scripts/create_infra.sh"

