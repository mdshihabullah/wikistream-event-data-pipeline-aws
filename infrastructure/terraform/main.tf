# =============================================================================
# WikiStream Pipeline - Terraform Configuration (Cost-Optimized)
# =============================================================================
# AWS streaming data pipeline with MSK (KRaft), EMR Serverless, S3 Tables
# Optimized for portfolio project with production-grade architecture
# =============================================================================

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  backend "s3" {
    bucket         = "wikistream-terraform-state-028210902207"
    key            = "wikistream/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "wikistream-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# =============================================================================
# DATA SOURCES
# =============================================================================

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  region      = data.aws_region.current.name
  name_prefix = "${var.project_name}-${var.environment}"
  azs         = slice(data.aws_availability_zones.available.names, 0, 2) # 2 AZs for cost savings
}

# =============================================================================
# VPC (Cost-Optimized: 2 AZs, Single NAT Gateway)
# =============================================================================

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.16.0"

  name = "${local.name_prefix}-vpc"
  cidr = var.vpc_cidr

  azs             = local.azs
  private_subnets = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 8, k)]
  public_subnets  = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 8, k + 10)]

  enable_nat_gateway   = true
  single_nat_gateway   = true # Cost saving: single NAT
  enable_dns_hostnames = true
  enable_dns_support   = true

  # No VPC Flow Logs for portfolio (cost saving)
  enable_flow_log = false
}

# =============================================================================
# SECURITY GROUPS
# =============================================================================

resource "aws_security_group" "msk" {
  name        = "${local.name_prefix}-msk-sg"
  description = "Security group for MSK cluster"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "Kafka from VPC"
    from_port   = 9092
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "ecs" {
  name        = "${local.name_prefix}-ecs-sg"
  description = "Security group for ECS Fargate tasks"
  vpc_id      = module.vpc.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "emr" {
  name        = "${local.name_prefix}-emr-sg"
  description = "Security group for EMR Serverless"
  vpc_id      = module.vpc.vpc_id

  # Self-referencing rule for internal communication
  ingress {
    description = "Allow internal communication"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  # Allow traffic from VPC for Kafka access
  ingress {
    description = "Kafka from VPC"
    from_port   = 9092
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# =============================================================================
# S3 BUCKETS
# =============================================================================

resource "aws_s3_bucket" "data" {
  bucket = "${local.name_prefix}-data-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "intelligent-tiering"
    status = "Enabled"

    filter {} # Required: empty filter applies to all objects

    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  rule {
    id     = "cleanup-old-checkpoints"
    status = "Enabled"
    filter {
      prefix = "checkpoints/"
    }
    expiration {
      days = 7
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# =============================================================================
# S3 TABLE BUCKET (Apache Iceberg) - Cost-Optimized Maintenance
# =============================================================================
# Based on AWS best practices for S3 Tables:
# - Compaction: Combines small files into larger ones (improves query performance)
# - Snapshot Management: Controls snapshot retention (reduces storage costs)
# - Unreferenced File Removal: Cleans up orphaned files (prevents cost accumulation)
# Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance.html
# =============================================================================

resource "aws_s3tables_table_bucket" "wikistream" {
  name = "${local.name_prefix}-tables"

  maintenance_configuration = {
    # Compaction: Merges small Parquet files into larger ones
    # - Improves query performance (fewer files to scan)
    # - Target 512MB files (good balance for analytics workloads)
    iceberg_compaction = {
      settings = {
        target_file_size_mb = 512
      }
      status = "enabled"
    }

    # Snapshot Management: Controls how many snapshots to retain
    # - Keeps storage costs down by expiring old snapshots
    # - min_snapshots_to_keep: 1 for dev (keep at least 1 for recovery)
    # - max_snapshot_age_hours: 48 (2 days) - allows time-travel for debugging in dev mode
    iceberg_snapshot_management = {
      settings = {
        min_snapshots_to_keep  = 1
        max_snapshot_age_hours = 48
      }
      status = "enabled"
    }

    # Unreferenced File Removal: Deletes orphaned files not referenced by any snapshot
    # - unreferenced_days: 3 days before cleanup (allows for job retries)
    # - non_current_days: 1 day for old file versions (aggressive for dev)
    iceberg_unreferenced_file_removal = {
      settings = {
        unreferenced_days = 3
        non_current_days  = 1
      }
      status = "enabled"
    }
  }
}

# S3 Tables Bucket Policy - Allow EMR Serverless Access
resource "aws_s3tables_table_bucket_policy" "wikistream" {
  table_bucket_arn = aws_s3tables_table_bucket.wikistream.arn
  resource_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowEMRServerlessFullAccess"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.emr_serverless.arn
        }
        Action = [
          "s3tables:*"
        ]
        Resource = "*"
      }
    ]
  })
}

# Namespaces for Medallion Architecture
resource "aws_s3tables_namespace" "bronze" {
  namespace        = "bronze"
  table_bucket_arn = aws_s3tables_table_bucket.wikistream.arn
}

resource "aws_s3tables_namespace" "silver" {
  namespace        = "silver"
  table_bucket_arn = aws_s3tables_table_bucket.wikistream.arn
}

resource "aws_s3tables_namespace" "gold" {
  namespace        = "gold"
  table_bucket_arn = aws_s3tables_table_bucket.wikistream.arn
}

resource "aws_s3tables_namespace" "dq_audit" {
  namespace        = "dq_audit"
  table_bucket_arn = aws_s3tables_table_bucket.wikistream.arn
}

# =============================================================================
# MSK CLUSTER (KRaft Mode - Kafka 3.9.0)
# =============================================================================

resource "aws_msk_configuration" "wikistream" {
  name           = "${local.name_prefix}-msk-config"
  kafka_versions = ["3.9.x"]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=true
    default.replication.factor=2
    min.insync.replicas=1
    num.partitions=6
    log.retention.hours=168
    log.retention.bytes=-1
    compression.type=snappy
  PROPERTIES
}

resource "aws_msk_cluster" "wikistream" {
  cluster_name           = "${local.name_prefix}-msk"
  kafka_version          = "3.9.x"
  number_of_broker_nodes = 2 # Minimum for KRaft (cost-optimized)

  broker_node_group_info {
    instance_type   = "kafka.t3.small" # Smallest instance for portfolio
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 50 # Minimum storage
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.wikistream.arn
    revision = aws_msk_configuration.wikistream.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.name_prefix}"
  retention_in_days = 7 # Short retention for portfolio
}

# =============================================================================
# IAM ROLES
# =============================================================================

# ECS Task Execution Role
resource "aws_iam_role" "ecs_execution" {
  name = "${local.name_prefix}-ecs-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ECS Task Role (for application permissions)
resource "aws_iam_role" "ecs_task" {
  name = "${local.name_prefix}-ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task_kafka" {
  name = "${local.name_prefix}-ecs-kafka-policy"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:AlterCluster"
        ]
        Resource = aws_msk_cluster.wikistream.arn
      },
      {
        Effect   = "Allow"
        Action   = ["kafka-cluster:*Topic*", "kafka-cluster:WriteData", "kafka-cluster:ReadData"]
        Resource = "arn:aws:kafka:${local.region}:${local.account_id}:topic/${local.name_prefix}-msk/*"
      },
      {
        Effect   = "Allow"
        Action   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
        Resource = "arn:aws:kafka:${local.region}:${local.account_id}:group/${local.name_prefix}-msk/*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

# EMR Serverless Role
resource "aws_iam_role" "emr_serverless" {
  name = "${local.name_prefix}-emr-serverless-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "emr_serverless" {
  name = "${local.name_prefix}-emr-serverless-policy"
  role = aws_iam_role.emr_serverless.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3DataBucketAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*"
        ]
      },
      {
        Sid    = "S3TablesFullAccess"
        Effect = "Allow"
        Action = [
          "s3tables:*"
        ]
        Resource = "*"
      },
      {
        Sid    = "S3TablesDataAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        # Note: S3 Tables uses AWS-managed S3 Express One Zone directory buckets
        # We need access to BOTH our account buckets AND AWS-managed internal storage
        Resource = [
          "arn:aws:s3:::*",
          "arn:aws:s3:::*/*"
        ]
      },
      {
        Sid    = "S3ExpressDataAccess"
        Effect = "Allow"
        Action = [
          "s3express:CreateSession",
          "s3express:GetObject",
          "s3express:PutObject",
          "s3express:DeleteObject",
          "s3express:ListBucket"
        ]
        # S3 Express actions for S3 Tables underlying storage
        Resource = "*"
      },
      {
        Sid    = "S3TablesUnderlyingStorage"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
          "s3:HeadObject",
          "s3:HeadBucket"
        ]
        # S3 Tables creates underlying storage buckets with two patterns:
        # 1. Pattern: s3tables-<random>-<account>-<region> (actual bucket)
        # 2. Pattern: <random>--table-s3 (virtual path in metadata)
        Resource = [
          "arn:aws:s3:::s3tables-*",
          "arn:aws:s3:::s3tables-*/*",
          "arn:aws:s3:::*--table-s3",
          "arn:aws:s3:::*--table-s3/*"
        ]
      },
      {
        Sid    = "MSKAccess"
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:*Topic*",
          "kafka-cluster:ReadData",
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup"
        ]
        Resource = "*"
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateDatabase",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          "arn:aws:glue:${local.region}:${local.account_id}:database/*",
          "arn:aws:glue:${local.region}:${local.account_id}:table/*"
        ]
      },
      {
        Sid      = "LakeFormationAccess"
        Effect   = "Allow"
        Action   = ["lakeformation:GetDataAccess"]
        Resource = "*"
      },
      {
        Sid      = "CloudWatchMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/emr-serverless/*",
          "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/emr-serverless/*:log-stream:*"
        ]
      },
      {
        Sid    = "CloudWatchLogsDescribe"
        Effect = "Allow"
        Action = [
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      },
      {
        Sid      = "SNSPublish"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

# =============================================================================
# ECS CLUSTER & SERVICE (Fargate - Producer)
# =============================================================================

resource "aws_ecs_cluster" "main" {
  name = "${local.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "disabled" # Cost saving for portfolio
  }
}

resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/ecs/${local.name_prefix}-producer"
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "producer" {
  family                   = "${local.name_prefix}-producer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256" # 0.25 vCPU - minimum
  memory                   = "512" # 0.5 GB - minimum
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "producer"
    image = "${local.account_id}.dkr.ecr.${local.region}.amazonaws.com/${local.name_prefix}-producer:latest"

    environment = [
      { name = "KAFKA_BOOTSTRAP_SERVERS", value = aws_msk_cluster.wikistream.bootstrap_brokers_sasl_iam },
      { name = "ENVIRONMENT", value = var.environment },
      { name = "AWS_REGION", value = local.region }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.ecs.name
        "awslogs-region"        = local.region
        "awslogs-stream-prefix" = "producer"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "pgrep -f kafka_producer.py || exit 1"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 60
    }
  }])
}

resource "aws_ecs_service" "producer" {
  name            = "${local.name_prefix}-producer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.producer.arn
  desired_count   = 0 # Start with 0, enable after image is pushed
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.private_subnets
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = false
  }

  # Ignore desired_count changes (managed via CLI/console)
  lifecycle {
    ignore_changes = [desired_count, task_definition]
  }

  depends_on = [aws_ecr_repository.producer]
}

# ECR Repository for Producer
resource "aws_ecr_repository" "producer" {
  name                 = "${local.name_prefix}-producer"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false # Cost saving
  }
}

resource "aws_ecr_lifecycle_policy" "producer" {
  repository = aws_ecr_repository.producer.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 3 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 3
      }
      action = { type = "expire" }
    }]
  })
}

# =============================================================================
# EMR SERVERLESS APPLICATION (Optimized for 32 vCPU quota)
# =============================================================================
# 
# IMPORTANT: Request quota increase before deploying!
# Default quota is 16 vCPU. Request 32 vCPU via Service Quotas console:
# 1. AWS Console → Service Quotas → EMR Serverless
# 2. Find "Max concurrent vCPUs per account" 
# 3. Request increase to 32 vCPU
# 4. Wait for approval (usually 24-48 hours for paid accounts)
#
# Resource allocation strategy:
# - Bronze streaming (continuous): 4 vCPU driver + 4 vCPU executors = 8 vCPU
# - Batch jobs run SEQUENTIALLY via unified Step Function:
#   - Silver: 2 vCPU driver + 4 vCPU executors = 6 vCPU
#   - Data Quality: 2 vCPU driver + 4 vCPU executors = 6 vCPU  
#   - Gold: 2 vCPU driver + 4 vCPU executors = 6 vCPU
# Total concurrent max: Bronze (8) + one batch job (6) = 14 vCPU (within 16 quota)
# With 32 quota: Bronze (8) + more executor headroom for spikes
# =============================================================================

resource "aws_emrserverless_application" "spark" {
  name          = "${local.name_prefix}-spark"
  release_label = "emr-7.12.0" # Iceberg format-version 3 support with Apache Iceberg 1.10.0
  type          = "SPARK"

  # Maximum capacity aligned with requested quota (32 vCPU)
  # This provides headroom for Bronze streaming + one batch job
  maximum_capacity {
    cpu    = "32 vCPU" # Request quota increase to 32 vCPU
    memory = "128 GB"  # 4 GB per vCPU ratio
    disk   = "400 GB"  # For Spark shuffle and temp data
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15 # Longer to reduce cold starts between batch jobs
  }

  network_configuration {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.emr.id]
  }

  # Pre-warm driver for faster job startup (reduces cold start latency)
  initial_capacity {
    initial_capacity_type = "DRIVER"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "4 GB"
        disk   = "20 GB"
      }
    }
  }

  # Pre-warm one executor to accelerate job starts
  initial_capacity {
    initial_capacity_type = "EXECUTOR"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "4 GB"
        disk   = "20 GB"
      }
    }
  }
}

# =============================================================================
# SNS TOPICS FOR ALERTING
# =============================================================================

resource "aws_sns_topic" "alerts" {
  name = "${local.name_prefix}-alerts"
}

# SNS Email Subscription for Pipeline Alerts
resource "aws_sns_topic_subscription" "alert_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# =============================================================================
# STEP FUNCTIONS (Alternative to MWAA - Cost-Effective Orchestration)
# =============================================================================

resource "aws_iam_role" "step_functions" {
  name = "${local.name_prefix}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${local.name_prefix}-sfn-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EMRServerlessJobManagement"
        Effect = "Allow"
        Action = [
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun",
          "emr-serverless:TagResource"
        ]
        Resource = [
          aws_emrserverless_application.spark.arn,
          "${aws_emrserverless_application.spark.arn}/jobruns/*"
        ]
      },
      {
        Sid      = "PassRoleToEMR"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = aws_iam_role.emr_serverless.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "emr-serverless.amazonaws.com"
          }
        }
      },
      {
        Sid      = "SNSPublish"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.alerts.arn
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      },
      {
        Sid    = "XRayAccess"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets"
        ]
        Resource = "*"
      },
      {
        Sid    = "EventBridgeManagedRules"
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
          "events:DeleteRule",
          "events:RemoveTargets"
        ]
        Resource = [
          "arn:aws:events:${local.region}:${local.account_id}:rule/StepFunctionsGetEventsForEMRServerlessJobRunRule",
          "arn:aws:events:${local.region}:${local.account_id}:rule/StepFunctions*"
        ]
      },
      {
        Sid      = "CloudWatchMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      }
    ]
  })
}

# =============================================================================
# UNIFIED BATCH PIPELINE STATE MACHINE WITH DQ GATES
# =============================================================================
# This state machine runs all batch jobs SEQUENTIALLY with DQ gates between layers.
# Flow: Bronze DQ Gate → Silver Job → Silver DQ Gate → Gold Job → Gold DQ Gate
# DQ gates block downstream processing if data quality checks fail.
# Total execution time target: ≤5 minutes to meet SLA
# =============================================================================

resource "aws_sfn_state_machine" "batch_pipeline" {
  name     = "${local.name_prefix}-batch-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = <<-EOF
{
  "Comment": "Unified batch pipeline with DQ gates: Bronze DQ → Silver → Silver DQ → Gold → Gold DQ",
  "StartAt": "RecordPipelineStart",
  "States": {
    "RecordPipelineStart": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [{
          "MetricName": "BatchPipelineStarted",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Pipeline", "Value": "batch"}]
        }]
      },
      "ResultPath": null,
      "Next": "BronzeDQGate"
    },
    "BronzeDQGate": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.spark.id}",
        "ExecutionRoleArn": "${aws_iam_role.emr_serverless.arn}",
        "Name": "bronze-dq-gate",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://${aws_s3_bucket.data.id}/spark/jobs/bronze_dq_gate.py",
            "EntryPointArguments": ["${aws_sns_topic.alerts.arn}", "2"],
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1 --py-files s3://${aws_s3_bucket.data.id}/spark/jobs/dq.zip"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "CloudWatchLoggingConfiguration": {
              "Enabled": true,
              "LogGroupName": "/aws/emr-serverless/${local.name_prefix}",
              "LogStreamNamePrefix": "bronze-dq-gate"
            },
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/bronze-dq-gate/"
            }
          }
        }
      },
      "ResultPath": "$.bronzeDQResult",
      "TimeoutSeconds": 180,
      "Next": "RecordBronzeDQSuccess",
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.error",
        "Next": "NotifyBronzeDQFailure"
      }]
    },
    "RecordBronzeDQSuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/DataQuality",
        "MetricData": [{
          "MetricName": "DQGatePassed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "bronze"}]
        }]
      },
      "ResultPath": null,
      "Next": "StartSilverJob"
    },
    "StartSilverJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.spark.id}",
        "ExecutionRoleArn": "${aws_iam_role.emr_serverless.arn}",
        "Name": "silver-processing",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://${aws_s3_bucket.data.id}/spark/jobs/silver_batch_job.py",
            "EntryPointArguments": ["1"],
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.adaptive.skewJoin.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "CloudWatchLoggingConfiguration": {
              "Enabled": true,
              "LogGroupName": "/aws/emr-serverless/${local.name_prefix}",
              "LogStreamNamePrefix": "silver"
            },
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/silver/"
            }
          }
        }
      },
      "ResultPath": "$.silverResult",
      "TimeoutSeconds": 180,
      "Next": "RecordSilverSuccess",
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.error",
        "Next": "NotifySilverFailure"
      }]
    },
    "RecordSilverSuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [{
          "MetricName": "SilverProcessingCompleted",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "silver"}]
        }]
      },
      "ResultPath": null,
      "Next": "SilverDQGate"
    },
    "SilverDQGate": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.spark.id}",
        "ExecutionRoleArn": "${aws_iam_role.emr_serverless.arn}",
        "Name": "silver-dq-gate",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://${aws_s3_bucket.data.id}/spark/jobs/silver_dq_gate.py",
            "EntryPointArguments": ["${aws_sns_topic.alerts.arn}", "2"],
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1 --py-files s3://${aws_s3_bucket.data.id}/spark/jobs/dq.zip"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "CloudWatchLoggingConfiguration": {
              "Enabled": true,
              "LogGroupName": "/aws/emr-serverless/${local.name_prefix}",
              "LogStreamNamePrefix": "silver-dq-gate"
            },
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/silver-dq-gate/"
            }
          }
        }
      },
      "ResultPath": "$.silverDQResult",
      "TimeoutSeconds": 180,
      "Next": "RecordSilverDQSuccess",
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.error",
        "Next": "NotifySilverDQFailure"
      }]
    },
    "RecordSilverDQSuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/DataQuality",
        "MetricData": [{
          "MetricName": "DQGatePassed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "silver"}]
        }]
      },
      "ResultPath": null,
      "Next": "StartGoldJob"
    },
    "StartGoldJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.spark.id}",
        "ExecutionRoleArn": "${aws_iam_role.emr_serverless.arn}",
        "Name": "gold-processing",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://${aws_s3_bucket.data.id}/spark/jobs/gold_batch_job.py",
            "EntryPointArguments": ["1"],
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.adaptive.skewJoin.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "CloudWatchLoggingConfiguration": {
              "Enabled": true,
              "LogGroupName": "/aws/emr-serverless/${local.name_prefix}",
              "LogStreamNamePrefix": "gold"
            },
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/gold/"
            }
          }
        }
      },
      "ResultPath": "$.goldResult",
      "TimeoutSeconds": 180,
      "Next": "RecordGoldSuccess",
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.error",
        "Next": "NotifyGoldFailure"
      }]
    },
    "RecordGoldSuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [{
          "MetricName": "GoldProcessingCompleted",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "gold"}]
        }]
      },
      "ResultPath": null,
      "Next": "GoldDQGate"
    },
    "GoldDQGate": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.spark.id}",
        "ExecutionRoleArn": "${aws_iam_role.emr_serverless.arn}",
        "Name": "gold-dq-gate",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://${aws_s3_bucket.data.id}/spark/jobs/gold_dq_gate.py",
            "EntryPointArguments": ["${aws_sns_topic.alerts.arn}", "2"],
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1 --py-files s3://${aws_s3_bucket.data.id}/spark/jobs/dq.zip"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "CloudWatchLoggingConfiguration": {
              "Enabled": true,
              "LogGroupName": "/aws/emr-serverless/${local.name_prefix}",
              "LogStreamNamePrefix": "gold-dq-gate"
            },
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/gold-dq-gate/"
            }
          }
        }
      },
      "ResultPath": "$.goldDQResult",
      "TimeoutSeconds": 180,
      "Next": "RecordPipelineComplete",
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.error",
        "Next": "NotifyGoldDQFailure"
      }]
    },
    "RecordPipelineComplete": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [
          {
            "MetricName": "BatchPipelineCompleted",
            "Value": 1,
            "Unit": "Count",
            "Dimensions": [{"Name": "Pipeline", "Value": "batch"}]
          },
          {
            "MetricName": "DQGatePassed",
            "Value": 1,
            "Unit": "Count",
            "Dimensions": [{"Name": "Layer", "Value": "gold"}]
          }
        ]
      },
      "ResultPath": null,
      "Next": "Success"
    },
    "NotifyBronzeDQFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.alerts.arn}",
        "Subject": "🚨 WikiStream Bronze DQ Gate FAILED",
        "Message.$": "States.Format('DQ_GATE_FAILURE | Layer: bronze | Severity: CRITICAL | ExecutionId: {} | Timestamp: {} | Error: {} | Action: Downstream processing blocked', $$.Execution.Id, $$.State.EnteredTime, $.error.Cause)"
      },
      "Next": "RecordBronzeDQFailure"
    },
    "RecordBronzeDQFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/DataQuality",
        "MetricData": [{
          "MetricName": "DQGateFailed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "bronze"}]
        }]
      },
      "ResultPath": null,
      "Next": "Fail"
    },
    "NotifySilverFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.alerts.arn}",
        "Subject": "WikiStream Silver Processing Failure",
        "Message.$": "States.Format('PIPELINE_FAILURE | Layer: silver | Severity: HIGH | ExecutionId: {} | Timestamp: {} | Error: {}', $$.Execution.Id, $$.State.EnteredTime, $.error.Cause)"
      },
      "Next": "RecordSilverFailure"
    },
    "RecordSilverFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [{
          "MetricName": "SilverProcessingFailed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "silver"}]
        }]
      },
      "ResultPath": null,
      "Next": "Fail"
    },
    "NotifySilverDQFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.alerts.arn}",
        "Subject": "🚨 WikiStream Silver DQ Gate FAILED",
        "Message.$": "States.Format('DQ_GATE_FAILURE | Layer: silver | Severity: CRITICAL | ExecutionId: {} | Timestamp: {} | Error: {} | Action: Gold processing blocked', $$.Execution.Id, $$.State.EnteredTime, $.error.Cause)"
      },
      "Next": "RecordSilverDQFailure"
    },
    "RecordSilverDQFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/DataQuality",
        "MetricData": [{
          "MetricName": "DQGateFailed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "silver"}]
        }]
      },
      "ResultPath": null,
      "Next": "Fail"
    },
    "NotifyGoldFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.alerts.arn}",
        "Subject": "WikiStream Gold Processing Failure",
        "Message.$": "States.Format('PIPELINE_FAILURE | Layer: gold | Severity: HIGH | ExecutionId: {} | Timestamp: {} | Error: {}', $$.Execution.Id, $$.State.EnteredTime, $.error.Cause)"
      },
      "Next": "RecordGoldFailure"
    },
    "RecordGoldFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [{
          "MetricName": "GoldProcessingFailed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "gold"}]
        }]
      },
      "ResultPath": null,
      "Next": "Fail"
    },
    "NotifyGoldDQFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.alerts.arn}",
        "Subject": "⚠️ WikiStream Gold DQ Gate FAILED",
        "Message.$": "States.Format('DQ_GATE_FAILURE | Layer: gold | Severity: HIGH | ExecutionId: {} | Timestamp: {} | Error: {} | Note: Data processed but quality checks failed', $$.Execution.Id, $$.State.EnteredTime, $.error.Cause)"
      },
      "Next": "RecordGoldDQFailure"
    },
    "RecordGoldDQFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/DataQuality",
        "MetricData": [{
          "MetricName": "DQGateFailed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "gold"}]
        }]
      },
      "ResultPath": null,
      "Next": "Fail"
    },
    "Fail": {
      "Type": "Fail",
      "Error": "BatchPipelineFailed",
      "Cause": "Batch pipeline or DQ gate failed"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
EOF
}

# Keep individual state machines for manual/ad-hoc runs
resource "aws_sfn_state_machine" "silver_processing" {
  name     = "${local.name_prefix}-silver-processing"
  role_arn = aws_iam_role.step_functions.arn

  definition = <<-EOF
{
  "Comment": "Silver layer batch processing (standalone) - Use batch-pipeline for scheduled runs",
  "StartAt": "StartSilverJob",
  "States": {
    "StartSilverJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.spark.id}",
        "ExecutionRoleArn": "${aws_iam_role.emr_serverless.arn}",
        "Name": "silver-processing",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://${aws_s3_bucket.data.id}/spark/jobs/silver_batch_job.py",
            "EntryPointArguments": ["1"],
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.adaptive.skewJoin.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "CloudWatchLoggingConfiguration": {
              "Enabled": true,
              "LogGroupName": "/aws/emr-serverless/${local.name_prefix}",
              "LogStreamNamePrefix": "silver"
            },
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/silver/"
            }
          }
        }
      },
      "ResultPath": "$.jobResult",
      "TimeoutSeconds": 180,
      "Next": "RecordSuccessMetrics",
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.error",
        "Next": "NotifyFailure"
      }]
    },
    "RecordSuccessMetrics": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [{
          "MetricName": "SilverProcessingCompleted",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "silver"}]
        }]
      },
      "ResultPath": null,
      "Next": "Success"
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.alerts.arn}",
        "Subject": "WikiStream Silver Processing Failure",
        "Message.$": "States.Format('PIPELINE_FAILURE | Layer: silver | Severity: HIGH | ExecutionId: {} | Timestamp: {} | Error: {}', $$.Execution.Id, $$.State.EnteredTime, $.error.Cause)"
      },
      "Next": "RecordFailureMetrics"
    },
    "RecordFailureMetrics": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [{
          "MetricName": "SilverProcessingFailed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "silver"}]
        }]
      },
      "ResultPath": null,
      "Next": "Fail"
    },
    "Fail": {
      "Type": "Fail",
      "Error": "SilverProcessingFailed",
      "Cause": "Silver layer batch processing failed"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
EOF
}

# Gold Processing State Machine (standalone - for manual/ad-hoc runs)
resource "aws_sfn_state_machine" "gold_processing" {
  name     = "${local.name_prefix}-gold-processing"
  role_arn = aws_iam_role.step_functions.arn

  definition = <<-EOF
{
  "Comment": "Gold layer batch processing (standalone) - Use batch-pipeline for scheduled runs",
  "StartAt": "StartGoldJob",
  "States": {
    "StartGoldJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.spark.id}",
        "ExecutionRoleArn": "${aws_iam_role.emr_serverless.arn}",
        "Name": "gold-processing",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://${aws_s3_bucket.data.id}/spark/jobs/gold_batch_job.py",
            "EntryPointArguments": ["1"],
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.adaptive.skewJoin.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "CloudWatchLoggingConfiguration": {
              "Enabled": true,
              "LogGroupName": "/aws/emr-serverless/${local.name_prefix}",
              "LogStreamNamePrefix": "gold"
            },
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/gold/"
            }
          }
        }
      },
      "ResultPath": "$.jobResult",
      "TimeoutSeconds": 180,
      "Next": "RecordSuccessMetrics",
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.error",
        "Next": "NotifyFailure"
      }]
    },
    "RecordSuccessMetrics": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [{
          "MetricName": "GoldProcessingCompleted",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "gold"}]
        }]
      },
      "ResultPath": null,
      "Next": "Success"
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.alerts.arn}",
        "Subject": "WikiStream Gold Processing Failure",
        "Message.$": "States.Format('PIPELINE_FAILURE | Layer: gold | Severity: HIGH | ExecutionId: {} | Timestamp: {} | Error: {}', $$.Execution.Id, $$.State.EnteredTime, $.error.Cause)"
      },
      "Next": "RecordFailureMetrics"
    },
    "RecordFailureMetrics": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/Pipeline",
        "MetricData": [{
          "MetricName": "GoldProcessingFailed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "gold"}]
        }]
      },
      "ResultPath": null,
      "Next": "Fail"
    },
    "Fail": {
      "Type": "Fail",
      "Error": "GoldProcessingFailed",
      "Cause": "Gold layer batch processing failed"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
EOF
}

# Data Quality State Machine (standalone - for manual/ad-hoc runs)
resource "aws_sfn_state_machine" "data_quality" {
  name     = "${local.name_prefix}-data-quality"
  role_arn = aws_iam_role.step_functions.arn

  definition = <<-EOF
{
  "Comment": "Data quality checks (standalone) - Use batch-pipeline for scheduled runs",
  "StartAt": "RunDataQualityJob",
  "States": {
    "RunDataQualityJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.spark.id}",
        "ExecutionRoleArn": "${aws_iam_role.emr_serverless.arn}",
        "Name": "data-quality-check",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://${aws_s3_bucket.data.id}/spark/jobs/data_quality_job.py",
            "EntryPointArguments": ["${aws_sns_topic.alerts.arn}"],
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0,com.amazon.deequ:deequ:2.0.7-spark-3.5 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "CloudWatchLoggingConfiguration": {
              "Enabled": true,
              "LogGroupName": "/aws/emr-serverless/${local.name_prefix}",
              "LogStreamNamePrefix": "data-quality"
            },
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/data-quality/"
            }
          }
        }
      },
      "ResultPath": "$.jobResult",
      "TimeoutSeconds": 300,
      "Next": "RecordQualityMetrics",
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.error",
        "Next": "NotifyQualityFailure"
      }]
    },
    "RecordQualityMetrics": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/DataQuality",
        "MetricData": [{
          "MetricName": "QualityCheckCompleted",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "all"}]
        }]
      },
      "ResultPath": null,
      "Next": "Success"
    },
    "NotifyQualityFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.alerts.arn}",
        "Subject": "WikiStream Data Quality Check Failure",
        "Message.$": "States.Format('DATA_QUALITY_FAILURE | Severity: CRITICAL | ExecutionId: {} | Timestamp: {} | Error: {}', $$.Execution.Id, $$.State.EnteredTime, $.error.Cause)"
      },
      "Next": "RecordQualityFailure"
    },
    "RecordQualityFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:putMetricData",
      "Parameters": {
        "Namespace": "WikiStream/DataQuality",
        "MetricData": [{
          "MetricName": "QualityCheckFailed",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [{"Name": "Layer", "Value": "all"}]
        }]
      },
      "ResultPath": null,
      "Next": "Fail"
    },
    "Fail": {
      "Type": "Fail",
      "Error": "DataQualityFailed",
      "Cause": "Data quality checks failed"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
EOF
}

# =============================================================================
# EVENTBRIDGE SCHEDULING (Single unified batch pipeline for ≤5 min SLA)
# =============================================================================
# Uses unified batch pipeline (Silver → DQ → Gold) to:
# 1. Stay within vCPU quota by running jobs sequentially
# 2. Meet ≤5 minute SLA for dashboard freshness
# 3. Reduce cold start overhead between jobs
# =============================================================================

# Primary schedule - Unified batch pipeline every 5 minutes
resource "aws_cloudwatch_event_rule" "batch_pipeline_schedule" {
  name                = "${local.name_prefix}-batch-pipeline-schedule"
  description         = "Trigger unified batch pipeline (Silver->DQ->Gold) every 5 minutes for ≤5 min SLA"
  schedule_expression = "rate(5 minutes)"
  state               = "DISABLED" # Enable via CLI after deployment
}

resource "aws_cloudwatch_event_target" "batch_pipeline_schedule" {
  rule      = aws_cloudwatch_event_rule.batch_pipeline_schedule.name
  target_id = "batch-pipeline"
  arn       = aws_sfn_state_machine.batch_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
}

# Keep individual schedules for manual overrides (DISABLED by default)
resource "aws_cloudwatch_event_rule" "silver_schedule" {
  name                = "${local.name_prefix}-silver-schedule"
  description         = "Individual Silver processing (use batch-pipeline for normal ops)"
  schedule_expression = "rate(5 minutes)"
  state               = "DISABLED"
}

resource "aws_cloudwatch_event_target" "silver_schedule" {
  rule      = aws_cloudwatch_event_rule.silver_schedule.name
  target_id = "silver-processing"
  arn       = aws_sfn_state_machine.silver_processing.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
}

resource "aws_cloudwatch_event_rule" "gold_schedule" {
  name                = "${local.name_prefix}-gold-schedule"
  description         = "Individual Gold processing (use batch-pipeline for normal ops)"
  schedule_expression = "rate(5 minutes)"
  state               = "DISABLED"
}

resource "aws_cloudwatch_event_target" "gold_schedule" {
  rule      = aws_cloudwatch_event_rule.gold_schedule.name
  target_id = "gold-processing"
  arn       = aws_sfn_state_machine.gold_processing.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
}

resource "aws_cloudwatch_event_rule" "data_quality_schedule" {
  name                = "${local.name_prefix}-data-quality-schedule"
  description         = "Individual Data Quality (use batch-pipeline for normal ops)"
  schedule_expression = "rate(15 minutes)"
  state               = "DISABLED"
}

resource "aws_cloudwatch_event_target" "data_quality_schedule" {
  rule      = aws_cloudwatch_event_rule.data_quality_schedule.name
  target_id = "data-quality"
  arn       = aws_sfn_state_machine.data_quality.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
}

resource "aws_iam_role" "eventbridge_sfn" {
  name = "${local.name_prefix}-eventbridge-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn" {
  name = "${local.name_prefix}-eventbridge-sfn-policy"
  role = aws_iam_role.eventbridge_sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["states:StartExecution"]
      Resource = [
        aws_sfn_state_machine.batch_pipeline.arn,
        aws_sfn_state_machine.silver_processing.arn,
        aws_sfn_state_machine.gold_processing.arn,
        aws_sfn_state_machine.data_quality.arn
      ]
    }]
  })
}

# =============================================================================
# CLOUDWATCH ALARMS
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "ecs_cpu" {
  alarm_name          = "${local.name_prefix}-producer-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ECS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    ClusterName = aws_ecs_cluster.main.name
    ServiceName = aws_ecs_service.producer.name
  }
}

# Bronze Job Health Alarm - Triggers when no records processed in 10 minutes
resource "aws_cloudwatch_metric_alarm" "bronze_health" {
  alarm_name          = "${local.name_prefix}-bronze-health"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "BronzeRecordsProcessed"
  namespace           = "WikiStream/Pipeline"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Bronze streaming job has stopped processing records - triggering auto-restart"
  alarm_actions       = [aws_sns_topic.alerts.arn, aws_lambda_function.bronze_restart.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "breaching"

  dimensions = {
    Layer = "bronze"
  }
}

# =============================================================================
# BRONZE JOB AUTO-RESTART LAMBDA
# =============================================================================

# Lambda function to restart Bronze streaming job on failure
resource "aws_lambda_function" "bronze_restart" {
  function_name = "${local.name_prefix}-bronze-restart"
  role          = aws_iam_role.bronze_restart_lambda.arn
  handler       = "index.handler"
  runtime       = "python3.12"
  timeout       = 60
  memory_size   = 128

  filename         = data.archive_file.bronze_restart_lambda.output_path
  source_code_hash = data.archive_file.bronze_restart_lambda.output_base64sha256

  environment {
    variables = {
      EMR_APP_ID    = aws_emrserverless_application.spark.id
      EMR_ROLE_ARN  = aws_iam_role.emr_serverless.arn
      S3_BUCKET     = aws_s3_bucket.data.id
      S3_TABLES_ARN = aws_s3tables_table_bucket.wikistream.arn
      MSK_BOOTSTRAP = aws_msk_cluster.wikistream.bootstrap_brokers_sasl_iam
      SNS_TOPIC_ARN = aws_sns_topic.alerts.arn
    }
  }
}

# Lambda code - inline for simplicity
# Optimized for 8 vCPU total (2 driver + 2 executors × 2 vCPU each)
data "archive_file" "bronze_restart_lambda" {
  type        = "zip"
  output_path = "${path.module}/bronze_restart_lambda.zip"

  source {
    content  = <<-PYTHON
import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

emr_client = boto3.client('emr-serverless')
sns_client = boto3.client('sns')

def handler(event, context):
    """
    Lambda handler to restart Bronze streaming job when CloudWatch alarm triggers.
    Optimized for 8 vCPU total: 2 vCPU driver + 2 executors × 2 vCPU each
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    app_id = os.environ['EMR_APP_ID']
    role_arn = os.environ['EMR_ROLE_ARN']
    s3_bucket = os.environ['S3_BUCKET']
    s3_tables_arn = os.environ['S3_TABLES_ARN']
    msk_bootstrap = os.environ['MSK_BOOTSTRAP']
    sns_topic = os.environ['SNS_TOPIC_ARN']
    
    try:
        # Check if Bronze job is already running
        response = emr_client.list_job_runs(
            applicationId=app_id,
            states=['RUNNING', 'PENDING', 'SUBMITTED']
        )
        
        bronze_jobs = [j for j in response.get('jobRuns', []) 
                       if j.get('name') == 'bronze-streaming']
        
        if bronze_jobs:
            logger.info(f"Bronze job already running: {bronze_jobs[0]['id']}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Bronze job already running', 'jobId': bronze_jobs[0]['id']})
            }
        
        # Start new Bronze streaming job with optimized resource allocation
        # Total: 8 vCPU = 2 vCPU driver + 2 executors × 2 vCPU
        iceberg_packages = (
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
            "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,"
            "software.amazon.awssdk:bundle:2.29.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "software.amazon.msk:aws-msk-iam-auth:2.2.0"
        )
        
        spark_conf = (
            f"--conf spark.driver.cores=2 "
            f"--conf spark.driver.memory=4g "
            f"--conf spark.executor.cores=2 "
            f"--conf spark.executor.memory=4g "
            f"--conf spark.executor.instances=2 "
            f"--conf spark.dynamicAllocation.enabled=false "
            f"--conf spark.jars.packages={iceberg_packages} "
            f"--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
            f"--conf spark.sql.adaptive.enabled=true "
            f"--conf spark.sql.adaptive.coalescePartitions.enabled=true "
            f"--conf spark.sql.iceberg.handle-timestamp-without-timezone=true "
            f"--conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog "
            f"--conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog "
            f"--conf spark.sql.catalog.s3tablesbucket.warehouse={s3_tables_arn} "
            f"--conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
        )
        
        response = emr_client.start_job_run(
            applicationId=app_id,
            executionRoleArn=role_arn,
            name='bronze-streaming',
            jobDriver={
                'sparkSubmit': {
                    'entryPoint': f's3://{s3_bucket}/spark/jobs/bronze_streaming_job.py',
                    'entryPointArguments': [msk_bootstrap, s3_bucket],
                    'sparkSubmitParameters': spark_conf
                }
            },
            configurationOverrides={
                'monitoringConfiguration': {
                    'cloudWatchLoggingConfiguration': {
                        'enabled': True,
                        'logGroupName': f'/aws/emr-serverless/wikistream-dev',
                        'logStreamNamePrefix': 'bronze'
                    },
                    's3MonitoringConfiguration': {
                        'logUri': f's3://{s3_bucket}/emr-serverless/logs/bronze/'
                    }
                }
            }
        )
        
        job_id = response['jobRunId']
        logger.info(f"Started Bronze job: {job_id}")
        
        # Send notification
        sns_client.publish(
            TopicArn=sns_topic,
            Subject='WikiStream Bronze Job Auto-Restarted',
            Message=json.dumps({
                'alert_type': 'BRONZE_JOB_RESTART',
                'severity': 'INFO',
                'job_id': job_id,
                'application_id': app_id,
                'resource_allocation': '8 vCPU (2 driver + 2x2 executor)',
                'message': 'Bronze streaming job was automatically restarted after health check failure'
            }, indent=2)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Bronze job restarted', 'jobId': job_id})
        }
        
    except Exception as e:
        logger.error(f"Error restarting Bronze job: {str(e)}")
        
        # Send error notification
        sns_client.publish(
            TopicArn=sns_topic,
            Subject='WikiStream Bronze Job Restart FAILED',
            Message=json.dumps({
                'alert_type': 'BRONZE_JOB_RESTART_FAILED',
                'severity': 'CRITICAL',
                'error': str(e),
                'message': 'Failed to restart Bronze streaming job - manual intervention required'
            }, indent=2)
        )
        
        raise
PYTHON
    filename = "index.py"
  }
}

# IAM Role for Bronze restart Lambda
resource "aws_iam_role" "bronze_restart_lambda" {
  name = "${local.name_prefix}-bronze-restart-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "bronze_restart_lambda" {
  name = "${local.name_prefix}-bronze-restart-lambda-policy"
  role = aws_iam_role.bronze_restart_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EMRServerlessAccess"
        Effect = "Allow"
        Action = [
          "emr-serverless:ListJobRuns",
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun"
        ]
        Resource = [
          aws_emrserverless_application.spark.arn,
          "${aws_emrserverless_application.spark.arn}/jobruns/*"
        ]
      },
      {
        Sid      = "PassRoleToEMR"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = aws_iam_role.emr_serverless.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "emr-serverless.amazonaws.com"
          }
        }
      },
      {
        Sid      = "SNSPublish"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.alerts.arn
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${local.region}:${local.account_id}:*"
      }
    ]
  })
}

# Permission for CloudWatch to invoke Lambda
resource "aws_lambda_permission" "cloudwatch_bronze_restart" {
  statement_id  = "AllowCloudWatchInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.bronze_restart.function_name
  principal     = "lambda.alarms.cloudwatch.amazonaws.com"
  source_arn    = aws_cloudwatch_metric_alarm.bronze_health.arn
}

# =============================================================================
# CLOUDWATCH LOG GROUP FOR EMR SERVERLESS
# =============================================================================

resource "aws_cloudwatch_log_group" "emr_serverless" {
  name              = "/aws/emr-serverless/${local.name_prefix}"
  retention_in_days = 14 # 2 weeks retention for debugging
}

# =============================================================================
# CLOUDWATCH DASHBOARD - PIPELINE MONITORING
# =============================================================================
# Comprehensive monitoring dashboard for the data pipeline
# Covers: Bronze streaming, Batch pipeline, Data quality, SLA metrics
# =============================================================================

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${local.name_prefix}-pipeline-dashboard"

  dashboard_body = jsonencode({
    widgets = [
      # Row 1: Pipeline Health Overview
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 6
        height = 6
        properties = {
          title  = "Bronze Streaming - Records/5min"
          region = local.region
          metrics = [
            ["WikiStream/Pipeline", "BronzeRecordsProcessed", "Layer", "bronze", { stat = "Sum", period = 300 }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 0
        width  = 6
        height = 6
        properties = {
          title  = "Batch Pipeline Executions"
          region = local.region
          metrics = [
            ["WikiStream/Pipeline", "BatchPipelineCompleted", "Pipeline", "batch", { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["WikiStream/Pipeline", "BatchPipelineStarted", "Pipeline", "batch", { stat = "Sum", period = 300, color = "#1f77b4" }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 6
        height = 6
        properties = {
          title  = "Processing Latency (ms)"
          region = local.region
          metrics = [
            ["WikiStream/Pipeline", "ProcessingLatencyMs", "Layer", "bronze", { stat = "Average", period = 300 }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0 } }
          annotations = {
            horizontal = [
              { value = 30000, label = "Target: 30s" }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 18
        y      = 0
        width  = 6
        height = 6
        properties = {
          title  = "Data Quality Status"
          region = local.region
          metrics = [
            ["WikiStream/DataQuality", "QualityCheckCompleted", "Layer", "all", { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["WikiStream/DataQuality", "QualityCheckFailed", "Layer", "all", { stat = "Sum", period = 300, color = "#d62728" }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0 } }
        }
      },
      # Row 2: Layer-specific Metrics
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 8
        height = 6
        properties = {
          title  = "Silver Processing"
          region = local.region
          metrics = [
            ["WikiStream/Pipeline", "SilverProcessingCompleted", "Layer", "silver", { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["WikiStream/Pipeline", "SilverProcessingFailed", "Layer", "silver", { stat = "Sum", period = 300, color = "#d62728" }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 6
        width  = 8
        height = 6
        properties = {
          title  = "Gold Processing"
          region = local.region
          metrics = [
            ["WikiStream/Pipeline", "GoldProcessingCompleted", "Layer", "gold", { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["WikiStream/Pipeline", "GoldProcessingFailed", "Layer", "gold", { stat = "Sum", period = 300, color = "#d62728" }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 6
        width  = 8
        height = 6
        properties = {
          title  = "Bronze Batch Completions"
          region = local.region
          metrics = [
            ["WikiStream/Pipeline", "BronzeBatchCompleted", "Layer", "bronze", { stat = "Sum", period = 300 }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0 } }
        }
      },
      # Row 3: Infrastructure Metrics
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 8
        height = 6
        properties = {
          title  = "ECS Producer - CPU/Memory"
          region = local.region
          metrics = [
            ["AWS/ECS", "CPUUtilization", "ClusterName", aws_ecs_cluster.main.name, "ServiceName", aws_ecs_service.producer.name, { stat = "Average", period = 300 }],
            ["AWS/ECS", "MemoryUtilization", "ClusterName", aws_ecs_cluster.main.name, "ServiceName", aws_ecs_service.producer.name, { stat = "Average", period = 300 }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0, max = 100 } }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 12
        width  = 8
        height = 6
        properties = {
          title  = "MSK - Bytes In/Out"
          region = local.region
          metrics = [
            ["AWS/Kafka", "BytesInPerSec", "Cluster Name", aws_msk_cluster.wikistream.cluster_name, { stat = "Average", period = 300 }],
            ["AWS/Kafka", "BytesOutPerSec", "Cluster Name", aws_msk_cluster.wikistream.cluster_name, { stat = "Average", period = 300 }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 12
        width  = 8
        height = 6
        properties = {
          title  = "Step Functions Executions"
          region = local.region
          metrics = [
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", aws_sfn_state_machine.batch_pipeline.arn, { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["AWS/States", "ExecutionsFailed", "StateMachineArn", aws_sfn_state_machine.batch_pipeline.arn, { stat = "Sum", period = 300, color = "#d62728" }],
            ["AWS/States", "ExecutionsStarted", "StateMachineArn", aws_sfn_state_machine.batch_pipeline.arn, { stat = "Sum", period = 300, color = "#1f77b4" }]
          ]
          view  = "timeSeries"
          yAxis = { left = { min = 0 } }
        }
      },
      # Row 4: Alerts and Text
      {
        type   = "text"
        x      = 0
        y      = 18
        width  = 12
        height = 3
        properties = {
          markdown = <<-EOF
## WikiStream Pipeline Health

**SLA Target:** Analytics dashboards reflect new edits within ≤5 minutes

**Architecture:**
- **Bronze (Streaming):** Kafka → Spark Streaming → Iceberg (30s micro-batches)
- **Batch Pipeline:** Silver → Data Quality → Gold (runs every 5 minutes)
- **EMR Serverless:** Optimized for 32 vCPU quota, jobs run sequentially

**Quick Links:**
- [EMR Serverless Console](https://${local.region}.console.aws.amazon.com/emr/home?region=${local.region}#/serverless/applications/${aws_emrserverless_application.spark.id})
- [Step Functions](https://${local.region}.console.aws.amazon.com/states/home?region=${local.region}#/statemachines)
EOF
        }
      },
      {
        type   = "alarm"
        x      = 12
        y      = 18
        width  = 12
        height = 3
        properties = {
          title = "Active Alarms"
          alarms = [
            aws_cloudwatch_metric_alarm.ecs_cpu.arn,
            aws_cloudwatch_metric_alarm.bronze_health.arn
          ]
        }
      }
    ]
  })
}

# =============================================================================
# SLA MONITORING ALARM - Pipeline End-to-End Latency
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "batch_pipeline_failure" {
  alarm_name          = "${local.name_prefix}-batch-pipeline-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Batch pipeline (Silver→DQ→Gold) has failed"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.batch_pipeline.arn
  }
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "msk_bootstrap_brokers_iam" {
  description = "MSK Bootstrap Brokers (IAM)"
  value       = aws_msk_cluster.wikistream.bootstrap_brokers_sasl_iam
  sensitive   = true
}

output "msk_cluster_arn" {
  description = "MSK Cluster ARN"
  value       = aws_msk_cluster.wikistream.arn
}

output "s3_tables_bucket_arn" {
  description = "S3 Tables Bucket ARN"
  value       = aws_s3tables_table_bucket.wikistream.arn
}

output "emr_serverless_app_id" {
  description = "EMR Serverless Application ID"
  value       = aws_emrserverless_application.spark.id
}

output "data_bucket" {
  description = "S3 Data Bucket"
  value       = aws_s3_bucket.data.id
}

output "ecr_repository_url" {
  description = "ECR Repository URL for Producer"
  value       = aws_ecr_repository.producer.repository_url
}

output "ecs_cluster_name" {
  description = "ECS Cluster Name"
  value       = aws_ecs_cluster.main.name
}

output "alerts_sns_topic_arn" {
  description = "SNS Topic ARN for alerts"
  value       = aws_sns_topic.alerts.arn
}

output "batch_pipeline_state_machine_arn" {
  description = "Unified Batch Pipeline State Machine ARN (Silver→DQ→Gold)"
  value       = aws_sfn_state_machine.batch_pipeline.arn
}

output "silver_state_machine_arn" {
  description = "Silver Processing State Machine ARN (standalone)"
  value       = aws_sfn_state_machine.silver_processing.arn
}

output "gold_state_machine_arn" {
  description = "Gold Processing State Machine ARN (standalone)"
  value       = aws_sfn_state_machine.gold_processing.arn
}

output "data_quality_state_machine_arn" {
  description = "Data Quality State Machine ARN (standalone)"
  value       = aws_sfn_state_machine.data_quality.arn
}

output "emr_serverless_role_arn" {
  description = "EMR Serverless Execution Role ARN"
  value       = aws_iam_role.emr_serverless.arn
}

output "bronze_restart_lambda_arn" {
  description = "Bronze Job Auto-Restart Lambda ARN"
  value       = aws_lambda_function.bronze_restart.arn
}

output "cloudwatch_dashboard_name" {
  description = "CloudWatch Dashboard name for pipeline monitoring"
  value       = aws_cloudwatch_dashboard.pipeline.dashboard_name
}

output "emr_serverless_log_group" {
  description = "CloudWatch Log Group for EMR Serverless jobs"
  value       = aws_cloudwatch_log_group.emr_serverless.name
}
