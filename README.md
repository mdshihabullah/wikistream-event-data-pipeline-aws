# WikiStream: Real-Time Wikipedia Edit Analytics Platform

A streaming data pipeline that captures live Wikipedia edits from Wikimedia EventStreams, processes them through a medallion architecture, and surfaces insights for content monitoring and risk detection.

**Tech Stack:** Amazon MSK (KRaft 3.9.x) · EMR Serverless (Spark 3.5) · AWS Step Functions · Amazon S3 Tables (Iceberg 1.10.0) · ECS Fargate · AWS Deequ  
**Deployment:** AWS Cloud (us-east-1) · Terraform 1.6+

> 📐 **For detailed architecture diagrams and component breakdown, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**

---

## Table of Contents

1. [Business Problem](#business-problem)
2. [Solution Overview](#solution-overview)
3. [Architecture](#architecture)
4. [Data Model](#data-model)
5. [Technology Choices](#technology-choices)
6. [Cost Estimation](#cost-estimation)
7. [Quick Start](#quick-start)
8. [Project Structure](#project-structure)

---

## Business Problem

### The Data Source

Wikimedia operates one of the largest collaborative platforms globally:

| Metric | Value |
|--------|-------|
| Wikipedia editions | 300+ languages |
| Edits per minute | ~500-700 (peak: 1,500+) |
| Monthly active editors | 280,000+ |
| Bot edits | ~20-30% of total |

**Wikimedia EventStreams** provides real-time Server-Sent Events (SSE) of all edits across all Wikimedia projects.

### The Use Cases

| Use Case | Description | Value |
|----------|-------------|-------|
| **Content Monitoring** | Track edit velocity, top contributors, trending pages | Understand community activity |
| **Vandalism Detection** | Identify suspicious edit patterns (rapid edits, large deletions) | Protect content quality |
| **Regional Analysis** | Compare activity across language editions | Geographic insights |
| **Bot Activity Tracking** | Monitor automated vs human contributions | Community health |

### Target Domains

We filter events from high-activity Wikipedia editions:

```
HIGH_ACTIVITY: en, de, ja, fr, zh, es, ru
REGIONAL: asia_pacific, europe, americas, middle_east
```

---

## Solution Overview

WikiStream implements the **Medallion Architecture** (Bronze → Silver → Gold) for progressive data refinement using **Apache Iceberg** format on **S3 Tables**:

```mermaid
flowchart LR
    subgraph Sources["📡 Source"]
        WM["Wikipedia<br/>EventStreams"]
    end

    subgraph Ingestion["📥 Ingestion"]
        ECS["ECS Fargate<br/>Producer"]
        MSK["Amazon MSK<br/>Kafka 3.9.x"]
    end

    subgraph Processing["⚡ EMR Serverless"]
        BRONZE["🥉 Bronze<br/>Streaming"]
        SILVER["🥈 Silver<br/>Batch"]
        DQ["🔍 Data Quality"]
        GOLD["🥇 Gold<br/>Batch"]
    end

    subgraph Storage["💾 S3 Tables"]
        ICE["Apache Iceberg<br/>Tables"]
    end

    WM -->|SSE| ECS
    ECS -->|Produce| MSK
    MSK -->|30s micro-batch| BRONZE
    BRONZE --> ICE
    ICE --> SILVER
    SILVER --> ICE
    ICE --> DQ
    ICE --> GOLD
    GOLD --> ICE
```

### Processing Flow with DQ Gates

| Layer | Job Type | Trigger | Description |
|-------|----------|---------|-------------|
| **Bronze** | Streaming | 30s micro-batches | Kafka → Iceberg with exactly-once semantics |
| **Bronze DQ Gate** | Batch | Before Silver | Completeness, timeliness (95% ≤3min), validity checks |
| **Silver** | Batch | After Bronze DQ passes | Deduplication, normalization, enrichment |
| **Silver DQ Gate** | Batch | After Silver | Accuracy, consistency, uniqueness, drift detection |
| **Gold** | Batch | After Silver DQ passes | Aggregations: hourly stats, entity trends, risk scores |
| **Gold DQ Gate** | Batch | After Gold | Upstream validation + aggregation consistency |

### Step Function Pipeline Flow

The batch pipeline is orchestrated by AWS Step Functions with a **self-looping continuous execution pattern**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BATCH PIPELINE STATE MACHINE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────┐                                                    │
│  │ RecordPipelineStart │ ← Initial trigger (after 15 min of Bronze data)   │
│  └──────────┬──────────┘                                                    │
│             ↓                                                               │
│  ┌──────────────────────┐      ❌ CATCH     ┌──────────────────────┐       │
│  │    BronzeDQGate      │ ────────────────→ │   NotifyFailure      │       │
│  │  (EMR Serverless)    │                   │   → RecordMetric     │       │
│  └──────────┬───────────┘                   │   → FAIL (stop loop) │       │
│             ↓ ✅                             └──────────────────────┘       │
│  ┌──────────────────────┐                                                   │
│  │   StartSilverJob     │ ──────────❌────→ Same failure pattern            │
│  └──────────┬───────────┘                                                   │
│             ↓ ✅                                                             │
│  ┌──────────────────────┐                                                   │
│  │    SilverDQGate      │ ──────────❌────→ Same failure pattern            │
│  └──────────┬───────────┘                                                   │
│             ↓ ✅                                                             │
│  ┌──────────────────────┐                                                   │
│  │    StartGoldJob      │ ──────────❌────→ Same failure pattern            │
│  └──────────┬───────────┘                                                   │
│             ↓ ✅                                                             │
│  ┌──────────────────────┐                                                   │
│  │     GoldDQGate       │ ──────────❌────→ Same failure pattern            │
│  └──────────┬───────────┘                                                   │
│             ↓ ✅ ALL GATES PASSED                                           │
│  ┌────────────────────────┐                                                 │
│  │ RecordPipelineComplete │                                                 │
│  └──────────┬─────────────┘                                                 │
│             ↓                                                               │
│  ┌────────────────────────┐                                                 │
│  │ WaitBeforeNextCycle    │  ⏱️ Wait 10 minutes                             │
│  └──────────┬─────────────┘                                                 │
│             ↓                                                               │
│  ┌────────────────────────┐                                                 │
│  │   TriggerNextCycle     │ ───→ StartExecution (self) ───→ END            │
│  └────────────────────────┘           ↑                                    │
│                                       │                                    │
│                              (New execution starts)                        │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│  TERMINAL STATES:                                                          │
│  • ✅ SUCCESS: TriggerNextCycle → starts new execution → continuous loop   │
│  • ❌ FAILURE: Fail state → stops loop → requires manual restart           │
└────────────────────────────────────────────────────────────────────────────┘
```

### Pipeline Timing

| Phase | Duration |
|-------|----------|
| Initial wait (Bronze data collection) | 15 min |
| Full batch pipeline execution | ~8-10 min |
| Wait between successful cycles | 10 min |
| **Total cycle time** | **~18-20 min** |

### Data Quality Gates

DQ gates block downstream processing when checks fail:

```
Bronze → [Bronze DQ Gate] → Silver → [Silver DQ Gate] → Gold → [Gold DQ Gate]
               ↓                            ↓                       ↓
           ❌ FAIL                      ❌ FAIL                  ❌ FAIL
      (stops pipeline)             (stops pipeline)         (stops pipeline)
```

| Gate | Check Types | Failure Action |
|------|-------------|----------------|
| **Bronze DQ** | Completeness (100% for IDs, 95% for optional), Timeliness (95% ≤3min) | Stops pipeline, SNS alert |
| **Silver DQ** | Accuracy, Consistency, Uniqueness, Drift detection | Stops pipeline, SNS alert |
| **Gold DQ** | Upstream verification, Aggregation consistency | Stops pipeline, SNS alert |

All DQ results are persisted to `dq_audit.quality_results` for audit and trend analysis.

### Key Features

- **≤30 second** Bronze ingestion latency (Spark Structured Streaming)
- **≤20 minute** end-to-end SLA for dashboard freshness (continuous batch cycles)
- **Exactly-once semantics** via Spark checkpointing and idempotent MERGE
- **Self-looping batch pipeline** with automatic 10-minute intervals between cycles
- **Fail-fast on DQ failure** - pipeline stops on any gate failure (prevents cascading bad data)
- **Auto-recovery** Lambda restarts Bronze streaming job on health check failure
- **Data quality gates** that block downstream processing on failures
- **DQ audit trail** with full evidence in Iceberg tables
- **SNS alerts** for DQ gate failures and pipeline errors

---

## Architecture

> 📐 **See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture diagrams, component breakdown, and data flow visualizations.**

### High-Level Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WikiStream Pipeline                             │
└─────────────────────────────────────────────────────────────────────────────┘

  INGESTION              STREAMING              PROCESSING           STORAGE
┌────────────┐        ┌────────────┐        ┌────────────────┐    ┌──────────┐
│ Wikipedia  │        │  Amazon    │        │ EMR Serverless │    │ S3 Tables│
│ SSE Feed   │──────▶│   MSK      │──────▶│                │───▶│ (Iceberg)│
└────────────┘        │  (Kafka)   │        │ Bronze→Silver  │    │          │
      │               └────────────┘        │ →DQ→Gold       │    │ bronze   │
      ▼                                     └────────────────┘    │ silver   │
┌────────────┐                                     │              │ gold     │
│ECS Fargate │                                     ▼              └──────────┘
│ Producer   │                              ┌────────────┐
└────────────┘                              │   Step     │
                                            │ Functions  │
                                            └────────────┘
```

### System Components

| Component | Service | Description |
|-----------|---------|-------------|
| **Data Source** | Wikipedia EventStreams | Real-time SSE feed |
| **Producer** | ECS Fargate | Python Kafka producer with IAM auth |
| **Message Broker** | Amazon MSK (KRaft 3.9.x) | 2 brokers, topics: `raw-events`, `dlq-events` |
| **Processing** | EMR Serverless (Spark 3.5) | Bronze streaming + Silver/Gold batch |
| **Storage** | S3 Tables (Iceberg 1.10.0) | Medallion architecture tables |
| **Orchestration** | Step Functions (self-looping) | Continuous batch pipeline with 10-min intervals |
| **Auto-Recovery** | Lambda + CloudWatch | Bronze job health monitoring |
| **Monitoring** | CloudWatch + SNS | Dashboard, metrics, alerts |

---

## Data Model

### Wikimedia Event Schema

Sample event from `stream.wikimedia.org/v2/stream/recentchange`:

```json
{
  "meta": {
    "id": "c3b60285-58c0-493e-ad60-ddab7732fcc4",
    "domain": "en.wikipedia.org",
    "dt": "2025-01-01T10:00:00Z"
  },
  "type": "edit",
  "title": "Example_Article",
  "user": "Editor123",
  "bot": false,
  "length": {"old": 1000, "new": 1050},
  "revision": {"old": 123456, "new": 123457}
}
```

### Medallion Tables

```mermaid
erDiagram
    BRONZE_RAW_EVENTS {
        string event_id PK
        string kafka_topic
        int kafka_partition
        bigint kafka_offset
        timestamp kafka_timestamp
        bigint rc_id
        string event_type
        int namespace
        string domain
        string title
        string title_url
        string user
        boolean is_bot
        string comment
        string wiki
        string server_name
        int length_old
        int length_new
        int length_delta
        bigint revision_old
        bigint revision_new
        timestamp event_timestamp
        timestamp producer_ingested_at
        timestamp bronze_processed_at
        string event_date "PARTITION"
        int event_hour "PARTITION"
        string schema_version
    }

    SILVER_CLEANED_EVENTS {
        string event_id PK
        string event_type
        string domain
        string region
        string language
        string user_normalized
        boolean is_bot
        boolean is_anonymous
        int length_delta
        boolean is_large_deletion
        boolean is_large_addition
        timestamp event_timestamp
        timestamp silver_processed_at
        string event_date "PARTITION"
        string schema_version
    }

    GOLD_HOURLY_STATS {
        string stat_date "PARTITION"
        int stat_hour
        string domain
        string region "PARTITION"
        bigint total_events
        bigint unique_users
        bigint unique_pages
        bigint bytes_added
        bigint bytes_removed
        double avg_edit_size
        bigint bot_edits
        bigint human_edits
        double bot_percentage
        bigint anonymous_edits
        bigint type_edit
        bigint type_new
        bigint type_categorize
        bigint type_log
        bigint large_deletions
        bigint large_additions
        timestamp gold_processed_at
        string schema_version
    }

    GOLD_RISK_SCORES {
        string stat_date "PARTITION"
        string entity_id
        string entity_type
        bigint total_edits
        double edits_per_hour_avg
        bigint large_deletions
        bigint domains_edited
        double risk_score
        string risk_level
        string evidence
        boolean alert_triggered
        timestamp gold_processed_at
        string schema_version
    }

    GOLD_DAILY_ANALYTICS_SUMMARY {
        string summary_date "PARTITION"
        bigint total_events
        bigint unique_users
        bigint active_domains
        bigint unique_pages_edited
        double bot_percentage
        double anonymous_percentage
        double registered_user_percentage
        bigint total_bytes_added
        bigint total_bytes_removed
        bigint net_content_change
        double avg_edit_size_bytes
        bigint new_pages_created
        bigint large_deletions_count
        double large_deletion_rate
        bigint high_risk_user_count
        bigint medium_risk_user_count
        bigint low_risk_user_count
        double platform_avg_risk_score
        double platform_max_risk_score
        bigint total_alerts_triggered
        double europe_percentage
        double americas_percentage
        double asia_pacific_percentage
        bigint peak_hour_events
        double avg_events_per_hour
        double platform_health_score
        timestamp gold_processed_at
        string schema_version
    }

    BRONZE_RAW_EVENTS ||--o{ SILVER_CLEANED_EVENTS : "transforms to"
    SILVER_CLEANED_EVENTS ||--o{ GOLD_HOURLY_STATS : "aggregates to"
    SILVER_CLEANED_EVENTS ||--o{ GOLD_RISK_SCORES : "generates"
    GOLD_HOURLY_STATS ||--o{ GOLD_DAILY_ANALYTICS_SUMMARY : "summarizes"
    GOLD_RISK_SCORES ||--o{ GOLD_DAILY_ANALYTICS_SUMMARY : "summarizes"
```

### Gold Tables Overview

| Table | Purpose | Key Metrics |
|-------|---------|-------------|
| **hourly_stats** | Hourly activity by domain/region | Events, users, bytes, bot %, edit types |
| **risk_scores** | User-level risk assessment | Risk score (0-100), risk level, evidence |
| **daily_analytics_summary** | Executive KPI dashboard | Platform health score, risk overview, trends |

### Risk Score Explained

The `gold.risk_scores` table identifies potentially problematic edit patterns:

| Risk Factor | Threshold | Points |
|-------------|-----------|--------|
| High edit velocity | >50 edits/hour | 40 |
| Large deletions | >3 large deletions | 30 |
| Anonymous activity | >50% anonymous | 20 |
| Cross-domain activity | >5 domains + high velocity | 10 |

**Risk Levels:**
- **HIGH** (70-100): Immediate attention needed
- **MEDIUM** (40-69): Monitor closely
- **LOW** (0-39): Normal activity

### Daily Analytics Summary Explained

The `gold.daily_analytics_summary` table provides a single-row-per-day executive dashboard with KPIs:

| Category | Metrics | Purpose |
|----------|---------|---------|
| **Volume KPIs** | total_events, unique_users, active_domains, unique_pages_edited | Activity overview |
| **User Mix** | bot_percentage, anonymous_percentage, registered_user_percentage | Community composition |
| **Content Health** | net_content_change, avg_edit_size_bytes, new_pages_created | Content growth |
| **Quality** | large_deletions_count, large_deletion_rate | Vandalism indicator |
| **Risk Overview** | high/medium/low_risk_user_count, platform_avg_risk_score, alerts_triggered | Security posture |
| **Regional** | europe/americas/asia_pacific_percentage | Geographic distribution |
| **Platform Health Score** | 0-100 composite score | Overall platform wellness |

**Platform Health Score (0-100):**

| Factor | Points | Calculation |
|--------|--------|-------------|
| Low risk users | 0-40 | 40 × (low_risk_users / total_scored_users) |
| Registered users | 0-30 | 30 × (registered_events / total_events) |
| Content growth | 0-20 | 20 if bytes_added > bytes_removed |
| Low deletion rate | 0-10 | 10 if large_deletions < 1% of events |

### Partitioning Strategy

| Table | Partition Columns | Rationale |
|-------|-------------------|-----------|
| **bronze.raw_events** | `(event_date, event_hour)` | Streaming micro-batches need time-based partitioning |
| **silver.cleaned_events** | `(event_date, region)` | Date + region for efficient regional time-range queries |
| **gold.hourly_stats** | `(stat_date, region)` | Date + region for dashboard drill-downs |
| **gold.risk_scores** | `(stat_date)` | Daily user risk assessments |
| **gold.daily_analytics_summary** | `(summary_date)` | One row per day for KPI trends |

---

## Technology Choices

| Component | Service | Why This Choice |
|-----------|---------|-----------------|
| **Message Queue** | Amazon MSK (KRaft 3.9.x) | Kafka without Zookeeper complexity. Native AWS IAM auth. |
| **Stream Producer** | ECS Fargate | Long-running SSE consumer. Pay-per-second, no server management. |
| **Bronze Processing** | EMR Serverless (Streaming) | Spark Structured Streaming with exactly-once semantics. Auto-restart via Lambda. |
| **Silver/Gold Processing** | EMR Serverless (Batch) | Zero idle cost. Pay only during job execution. Auto-scaling Spark. |
| **Table Format** | S3 Tables (Iceberg 1.10.0) | AWS-managed Iceberg with ACID transactions, time-travel, automatic compaction. |
| **Data Quality** | AWS Deequ 2.0.7 | Native Spark integration. Completeness, validity, uniqueness checks. |
| **Orchestration** | Step Functions (self-looping) | Serverless. Continuous batch pipeline with 10-min intervals. Fail-fast on errors. |
| **Auto-Recovery** | Lambda | Restarts Bronze job on CloudWatch alarm trigger |

### Why Not...?

| Alternative | Reason Not Used |
|-------------|-----------------|
| MSK Serverless | Less control over configuration; provisioned is more predictable |
| MWAA (Airflow) | $250+/month minimum; Step Functions is 90% cheaper |
| Lambda for Producer | 15-minute timeout; SSE stream requires continuous connection |
| Kinesis | MSK provides more flexibility with Kafka ecosystem |
| All Streaming | Silver/Gold transformations don't benefit from streaming; batch is more cost-effective |
| dbt | Adds operational complexity. Native Spark SQL + Step Functions provides equivalent functionality |

---

## Cost Estimation

### Monthly Cost (Dev/Portfolio)

| Service | Configuration | Est. Cost |
|---------|---------------|-----------|
| MSK | 2× kafka.t3.small, 50GB each | ~$90 |
| ECS Fargate | 0.25 vCPU, 0.5GB, 24/7 (Producer) | ~$10 |
| EMR Serverless - Bronze | Streaming job, pre-warmed workers | ~$100 |
| EMR Serverless - Silver/Gold | Batch jobs via Step Functions | ~$15 |
| S3 Tables + S3 | ~10 GB storage | ~$3 |
| NAT Gateway | Single AZ | ~$35 |
| Step Functions + Lambda | Orchestration + auto-restart | ~$2 |
| CloudWatch | Logs + Metrics | ~$5 |
| **Total (24/7)** | | **~$260/month** |

### Cost Optimization Scripts

For development, use the provided scripts to destroy/recreate costly infrastructure:

```bash
# End of day - saves ~$19/day by destroying NAT, MSK, EMR
./scripts/destroy_infra.sh

# Start of day - recreates infrastructure (~25-35 min)
./scripts/create_infra.sh
```

**Preserved during destroy:** S3 buckets, S3 Tables (your data), ECR images, Terraform state

---

## Quick Start

### Prerequisites

```bash
aws --version          # AWS CLI v2.x required
terraform --version    # Terraform >= 1.6.0
docker --version       # Docker for building images
```

### Deploy

```bash
# 1. Setup Terraform backend (first time only)
./scripts/setup_terraform_backend.sh

# 2. Deploy infrastructure
cd infrastructure/terraform
terraform init
terraform apply

# 3. Build and push producer image
ECR_URL=$(terraform output -raw ecr_repository_url)
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(echo $ECR_URL | cut -d'/' -f1)
cd ../../producer
docker build --platform linux/amd64 -t ${ECR_URL}:latest .
docker push ${ECR_URL}:latest

# 4. Upload Spark jobs
DATA_BUCKET=$(terraform -chdir=../infrastructure/terraform output -raw data_bucket)
aws s3 sync ../spark/jobs/ s3://${DATA_BUCKET}/spark/jobs/

# 5. Start services
./scripts/create_infra.sh  # Starts ECS producer + Bronze streaming job + schedules batch pipeline
```

> **Note:** The batch pipeline automatically starts 15 minutes after `create_infra.sh` to allow Bronze layer to collect sufficient data. It then runs continuously with 10-minute intervals between cycles.

### Verify Pipeline

```bash
# Check EMR jobs
EMR_APP_ID=$(terraform -chdir=infrastructure/terraform output -raw emr_serverless_app_id)
aws emr-serverless list-job-runs --application-id ${EMR_APP_ID} --output table

# Check producer logs
aws logs tail /ecs/wikistream-dev-producer --follow --since 5m

# Check Step Functions executions
aws stepfunctions list-executions \
  --state-machine-arn $(terraform -chdir=infrastructure/terraform output -raw batch_pipeline_state_machine_arn) \
  --max-results 5
```

---

## Monitoring & Analytics

WikiStream provides two types of analytics:

### Operational Monitoring (Grafana)

Local dockerized Grafana connected to CloudWatch for pipeline health:

```bash
# Start Grafana
cd monitoring/docker
docker-compose up -d

# Access at http://localhost:3000 (admin/wikistream)
```

**Dashboard includes:**
- DQ Gate status (Bronze/Silver/Gold pass/fail)
- Processing latency and throughput
- Pipeline failures and trends
- Infrastructure metrics (MSK, ECS, EMR)

### Business Analytics (QuickSight)

AWS QuickSight dashboards for stakeholder insights:
- Edit activity trends
- Regional and language analysis
- Risk score monitoring
- Contributor patterns

---

## Project Structure

```
wikistream/
├── README.md                          # This file
├── docs/
│   ├── ARCHITECTURE.md                # Detailed architecture documentation
│   └── architecture_diagram.html      # Interactive HTML diagram
├── infrastructure/
│   └── terraform/
│       ├── main.tf                    # AWS resources (VPC, MSK, EMR, ECS, Step Functions)
│       └── variables.tf               # Configuration variables
├── producer/
│   ├── kafka_producer.py              # SSE consumer → Kafka producer
│   ├── requirements.txt               # Python dependencies
│   └── Dockerfile                     # Container image for ECS
├── spark/
│   ├── jobs/
│   │   ├── bronze_streaming_job.py    # Kafka → Bronze (Spark Structured Streaming)
│   │   ├── silver_batch_job.py        # Bronze → Silver (Spark Batch)
│   │   ├── gold_batch_job.py          # Silver → Gold (Spark Batch)
│   │   ├── bronze_dq_gate.py          # Bronze layer DQ gate job
│   │   ├── silver_dq_gate.py          # Silver layer DQ gate job
│   │   ├── gold_dq_gate.py            # Gold layer DQ gate job
│   │   └── dq/                        # DQ module
│   │       ├── __init__.py
│   │       ├── dq_checks.py           # Deequ-based DQ check classes
│   │       └── dq_utils.py            # Audit writer, metrics, alerting
│   └── schemas/
│       ├── bronze_schema.py           # Raw event schema
│       ├── silver_schema.py           # Cleaned event schema
│       └── gold_schema.py             # Aggregation schemas
├── config/
│   └── settings.py                    # Domain filters, regions, SLAs
├── scripts/
│   ├── setup_terraform_backend.sh     # Setup S3 + DynamoDB for state
│   ├── create_infra.sh                # Start infrastructure + pipeline
│   ├── destroy_infra.sh               # Stop costly resources (preserves data)
│   └── deploy.sh                      # Full deployment script
├── monitoring/
│   ├── docker/                        # Dockerized Grafana setup
│   │   ├── docker-compose.yml
│   │   └── grafana/provisioning/      # Auto-provisioned datasources
│   ├── grafana/dashboards/            # Pipeline health dashboard
│   └── cloudwatch/alarms.json         # CloudWatch alarm definitions
└── quicksight/
    ├── datasets/                      # QuickSight dataset configs
    └── dashboards/                    # Dashboard definitions
```

---

## Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| **ECS Kafka Producer** | ✅ Implemented | Python, IAM auth, DLQ support |
| **MSK Cluster (KRaft)** | ✅ Implemented | Kafka 3.9.x, 2 brokers |
| **Bronze Streaming Job** | ✅ Implemented | 30s micro-batches, MERGE INTO |
| **Silver Batch Job** | ✅ Implemented | Deduplication, normalization |
| **Gold Batch Job** | ✅ Implemented | Aggregations, risk scores, daily analytics summary |
| **Bronze DQ Gate** | ✅ Implemented | Completeness, timeliness (95% ≤3min), validity |
| **Silver DQ Gate** | ✅ Implemented | Accuracy, consistency, uniqueness, drift detection |
| **Gold DQ Gate** | ✅ Implemented | Upstream validation, aggregation consistency |
| **DQ Audit Tables** | ✅ Implemented | `dq_audit.quality_results`, `dq_audit.profile_metrics` |
| **Step Functions Pipeline** | ✅ Implemented | Self-looping: Bronze DQ → Silver → Silver DQ → Gold → Gold DQ → Wait 10min → Repeat |
| **Auto-Restart Lambda** | ✅ Implemented | CloudWatch alarm trigger |
| **CloudWatch Dashboard** | ✅ Implemented | Pipeline health + DQ gate metrics |
| **Grafana (Docker)** | ✅ Implemented | Local operational monitoring |
| **SNS Alerts** | ✅ Implemented | Failure notifications to mrshihabullah@gmail.com |
| **S3 Tables (Iceberg)** | ✅ Implemented | bronze, silver, gold, dq_audit namespaces |
| **Cost Optimization Scripts** | ✅ Implemented | destroy/create for dev workflow |

---

## Author

Built as a portfolio project demonstrating modern data engineering on AWS.

**Skills Demonstrated:**
- Real-time streaming with Kafka (MSK KRaft 3.9.x)
- Apache Iceberg 1.10.0 on S3 Tables
- Serverless Spark (EMR Serverless 7.12.0)
- Infrastructure as Code (Terraform)
- Medallion Architecture (Bronze streaming, Silver/Gold batch)
- Data Quality (AWS Deequ)
- Self-healing infrastructure (Lambda auto-restart)
- Cost-optimized dev workflow
