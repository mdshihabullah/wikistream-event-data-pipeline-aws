#!/bin/bash
set -e

# Configuration
NAME_PREFIX="wikistream-dev"
DATA_BUCKET="s3://wikistream-dev-data-160884803380/spark/jobs/dq.zip"

echo "========================================="
echo "Packaging and Uploading DQ Module"
echo "========================================="

# Create zip file
cd /Users/shihab/Documents/Neue_Fische/wikistream/spark/jobs/dq
python3 << 'PYEOF'
import os
import zipfile

with zipfile.ZipFile('dq.zip', 'w') as z:
    z.write('dq_checks.py')
    z.write('dq_utils.py')
    z.write('__init__.py')
print('Created dq.zip')
PYEOF

echo "✅ Created: spark/jobs/dq/dq.zip"

# Upload to S3
echo ""
echo "Uploading to S3..."
AWS_PROFILE=neuefische AWS_REGION=us-east-1 aws s3 cp dq.zip "$DATA_BUCKET"

if [ $? -eq 0 ]; then
    echo "✅ Upload successful!"
else
    echo "❌ Upload failed!"
    exit 1
fi

# Trigger Step Function pipeline
echo ""
echo "========================================="
echo "Restarting Batch Pipeline"
echo "========================================="

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
EXECUTION_NAME="${NAME_PREFIX}-manual-restart-${TIMESTAMP}"

aws stepfunctions start-execution \
    --profile neuefische \
    --region us-east-1 \
    --state-machine-arn arn:aws:states:us-east-1:160884803380:stateMachine:wikistream-dev-batch-pipeline \
    --name "$EXECUTION_NAME" \
    --input '{"triggered_by": "manual_restart", "initial_start": true, "environment": "dev"}'

if [ $? -eq 0 ]; then
    echo "✅ Step Function execution started!"
    echo "Execution name: $EXECUTION_NAME"
else
    echo "❌ Failed to trigger Step Function!"
    exit 1
fi

echo ""
echo "========================================="
echo "Next Steps"
echo "========================================="
echo "1. Monitor pipeline execution:"
echo "   AWS_PROFILE=neuefische AWS_REGION=us-east-1 aws stepfunctions list-executions \\"
echo "      --state-machine-arn arn:aws:states:us-east-1:160884803380:stateMachine:wikistream-dev-batch-pipeline \\"
echo "      --max-results 5"
echo ""
echo "2. Check CloudWatch logs:"
echo "   AWS_PROFILE=neuefische AWS_REGION=us-east-1 aws logs tail /aws/emr-serverless/wikistream-dev --log-stream-name-prefix silver-dq-gate --since 5m"
echo ""
echo "3. If Silver DQ passes, pipeline will continue to Gold processing!"
