#!/bin/bash
# =============================================================================
# WikiStream Pipeline - Replay Script
# =============================================================================
# This script replays Kafka messages for reprocessing.
# Uses consumer offsets to replay from a specific point.
# Idempotent processing ensures no duplicate records.
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
REPLAY_FROM="${1:-}"  # timestamp or offset
CONSUMER_GROUP="${2:-wikistream-bronze-consumer}"
MSK_BOOTSTRAP="${MSK_BOOTSTRAP:-}"
AWS_REGION="${AWS_REGION:-us-east-1}"
TOPIC="wikistream.raw.events"

# Validate inputs
if [[ -z "$REPLAY_FROM" ]]; then
    echo -e "${RED}Usage: $0 <replay_from> [consumer_group]${NC}"
    echo -e "  replay_from: timestamp (YYYY-MM-DDTHH:MM:SS) or 'earliest'"
    echo -e "  consumer_group: Kafka consumer group (default: wikistream-bronze-consumer)"
    exit 1
fi

if [[ -z "$MSK_BOOTSTRAP" ]]; then
    echo -e "${RED}Error: MSK_BOOTSTRAP environment variable not set${NC}"
    exit 1
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}WikiStream Kafka Replay${NC}"
echo -e "${GREEN}Replay From: ${REPLAY_FROM}${NC}"
echo -e "${GREEN}Consumer Group: ${CONSUMER_GROUP}${NC}"
echo -e "${GREEN}Topic: ${TOPIC}${NC}"
echo -e "${GREEN}========================================${NC}"

# =============================================================================
# Step 1: Stop Current Consumers
# =============================================================================
echo -e "\n${YELLOW}Step 1: Ensuring consumers are stopped...${NC}"
echo -e "${YELLOW}Note: Stop the Bronze streaming job before replay${NC}"
echo -e "${YELLOW}Either through Spark UI or by terminating the EMR step${NC}"

read -p "Have you stopped the Bronze streaming job? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${RED}Please stop the streaming job first${NC}"
    exit 1
fi

# =============================================================================
# Step 2: Reset Consumer Offsets
# =============================================================================
echo -e "\n${YELLOW}Step 2: Resetting consumer offsets...${NC}"

if [[ "$REPLAY_FROM" == "earliest" ]]; then
    # Reset to earliest
    echo -e "Resetting to earliest offset..."
    
    # Using kafka-consumer-groups.sh (requires Kafka tools)
    # For MSK, you may need to use AWS CLI or a Kafka client
    cat << EOF
To reset offsets to earliest, run the following in a Kafka client environment:

kafka-consumer-groups.sh \\
    --bootstrap-server ${MSK_BOOTSTRAP} \\
    --group ${CONSUMER_GROUP} \\
    --topic ${TOPIC} \\
    --reset-offsets \\
    --to-earliest \\
    --execute \\
    --command-config /path/to/client.properties

Or use the Spark job with 'startingOffsets': 'earliest' configuration.
EOF

else
    # Reset to timestamp
    echo -e "Resetting to timestamp: ${REPLAY_FROM}..."
    
    # Convert timestamp to milliseconds
    TIMESTAMP_MS=$(date -d "$REPLAY_FROM" +%s)000
    
    cat << EOF
To reset offsets to timestamp ${REPLAY_FROM}, run:

kafka-consumer-groups.sh \\
    --bootstrap-server ${MSK_BOOTSTRAP} \\
    --group ${CONSUMER_GROUP} \\
    --topic ${TOPIC} \\
    --reset-offsets \\
    --to-datetime ${REPLAY_FROM}T00:00:00.000 \\
    --execute \\
    --command-config /path/to/client.properties

EOF
fi

# =============================================================================
# Step 3: Verify Offset Reset
# =============================================================================
echo -e "\n${YELLOW}Step 3: Verify offset reset${NC}"

cat << EOF
Verify offsets with:

kafka-consumer-groups.sh \\
    --bootstrap-server ${MSK_BOOTSTRAP} \\
    --group ${CONSUMER_GROUP} \\
    --describe \\
    --command-config /path/to/client.properties

Expected: CURRENT-OFFSET should match the target position
EOF

# =============================================================================
# Step 4: Restart Consumers
# =============================================================================
echo -e "\n${YELLOW}Step 4: Restart the Bronze streaming job${NC}"

cat << EOF
Restart options:

1. Via Airflow: Trigger the Bronze streaming DAG manually
2. Via EMR: Submit a new Bronze streaming step
3. Via Spark: spark-submit the bronze_streaming_job.py

The job will automatically pick up from the reset offset position.
Idempotent MERGE operations ensure no duplicate records.
EOF

# =============================================================================
# Step 5: Monitor Replay Progress
# =============================================================================
echo -e "\n${YELLOW}Step 5: Monitor replay progress${NC}"

cat << EOF
Monitor replay progress:

1. Kafka lag:
   - CloudWatch: AWS/Kafka -> SumOffsetLag
   - Grafana: WikiStream Pipeline Health dashboard

2. Processing metrics:
   - CloudWatch: WikiStream/Pipeline -> BronzeProcessingCompleted
   - EMR Spark UI: Application progress

3. Data verification:
   - Query Bronze table for expected date range
   - Check record counts match Kafka message counts

EOF

# =============================================================================
# Summary
# =============================================================================
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Replay Instructions Complete${NC}"
echo -e "${GREEN}========================================${NC}"

echo -e "\n${YELLOW}Important Notes:${NC}"
echo -e "1. Replay uses idempotent MERGE operations - no duplicates"
echo -e "2. Downstream Silver/Gold will auto-process new Bronze data"
echo -e "3. Monitor DLQ for any failed messages during replay"
echo -e "4. Expect increased processing time during catch-up"

echo -e "\n${GREEN}Done!${NC}"



