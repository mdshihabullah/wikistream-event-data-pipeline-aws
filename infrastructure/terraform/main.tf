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
# S3 TABLE BUCKET (Apache Iceberg)
# =============================================================================

resource "aws_s3tables_table_bucket" "wikistream" {
  name = "${local.name_prefix}-tables"

  maintenance_configuration = {
    iceberg_unreferenced_file_removal = {
      settings = {
        unreferenced_days = 7
        non_current_days  = 3
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
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/emr-serverless/*",
          "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/emr-serverless/*:log-stream:*"
        ]
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
# EMR SERVERLESS APPLICATION
# =============================================================================

resource "aws_emrserverless_application" "spark" {
  name          = "${local.name_prefix}-spark"
  release_label = "emr-7.12.0" # Iceberg format-version 3 support with Apache Iceberg 1.10.0
  type          = "SPARK"

  # Account quota is 16 vCPUs - optimize for sequential job execution
  # Bronze streaming: ~4 vCPUs, Batch jobs: ~8 vCPUs each
  maximum_capacity {
    cpu    = "16 vCPU"  # Matches account quota - run jobs sequentially
    memory = "64 GB"
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 10 # Slightly longer to reduce cold starts between jobs
  }

  network_configuration {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.emr.id]
  }

  # Pre-warm driver for faster job startup
  initial_capacity {
    initial_capacity_type = "DRIVER"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "4 GB"
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

# Silver Processing State Machine
resource "aws_sfn_state_machine" "silver_processing" {
  name     = "${local.name_prefix}-silver-processing"
  role_arn = aws_iam_role.step_functions.arn

  definition = <<-EOF
{
  "Comment": "Silver layer batch processing with data quality checks",
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
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=2 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.adaptive.skewJoin.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/"
            }
          }
        }
      },
      "ResultPath": "$.jobResult",
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
        "Message.$": "States.Format('{{\"alert_type\":\"PIPELINE_FAILURE\",\"severity\":\"HIGH\",\"layer\":\"silver\",\"execution_id\":\"{}\",\"error\":\"{}\",\"timestamp\":\"{}\",\"runbook\":\"https://wiki.example.com/wikistream/runbook#silver-failure\"}}', $$.Execution.Id, $.error.Cause, $$.State.EnteredTime)"
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

# Gold Processing State Machine  
resource "aws_sfn_state_machine" "gold_processing" {
  name     = "${local.name_prefix}-gold-processing"
  role_arn = aws_iam_role.step_functions.arn

  definition = <<-EOF
{
  "Comment": "Gold layer batch processing with analytics aggregations",
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
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=2 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.adaptive.skewJoin.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/"
            }
          }
        }
      },
      "ResultPath": "$.jobResult",
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
        "Message.$": "States.Format('{{\"alert_type\":\"PIPELINE_FAILURE\",\"severity\":\"HIGH\",\"layer\":\"gold\",\"execution_id\":\"{}\",\"error\":\"{}\",\"timestamp\":\"{}\",\"runbook\":\"https://wiki.example.com/wikistream/runbook#gold-failure\"}}', $$.Execution.Id, $.error.Cause, $$.State.EnteredTime)"
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

# Data Quality State Machine
resource "aws_sfn_state_machine" "data_quality" {
  name     = "${local.name_prefix}-data-quality"
  role_arn = aws_iam_role.step_functions.arn

  definition = <<-EOF
{
  "Comment": "Data quality checks using Deequ across all layers",
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
            "SparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=2 --conf spark.dynamicAllocation.enabled=false --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.0,com.amazon.deequ:deequ:2.0.7-spark-3.5 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.adaptive.skewJoin.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${aws_s3tables_table_bucket.wikistream.arn} --conf spark.sql.catalog.s3tablesbucket.client.region=us-east-1"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data.id}/emr-serverless/logs/"
            }
          }
        }
      },
      "ResultPath": "$.jobResult",
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
        "Message.$": "States.Format('{{\"alert_type\":\"DATA_QUALITY_FAILURE\",\"severity\":\"CRITICAL\",\"execution_id\":\"{}\",\"error\":\"{}\",\"timestamp\":\"{}\",\"layers_affected\":[\"bronze\",\"silver\",\"gold\"],\"runbook\":\"https://wiki.example.com/wikistream/runbook#data-quality\"}}', $$.Execution.Id, $.error.Cause, $$.State.EnteredTime)"
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

# EventBridge Rules for Scheduling (Start DISABLED - enable after deployment)
resource "aws_cloudwatch_event_rule" "silver_schedule" {
  name                = "${local.name_prefix}-silver-schedule"
  description         = "Trigger Silver processing every 5 minutes"
  schedule_expression = "rate(5 minutes)"
  state               = "DISABLED" # Enable via CLI after image is pushed
}

resource "aws_cloudwatch_event_target" "silver_schedule" {
  rule      = aws_cloudwatch_event_rule.silver_schedule.name
  target_id = "silver-processing"
  arn       = aws_sfn_state_machine.silver_processing.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
}

resource "aws_cloudwatch_event_rule" "gold_schedule" {
  name                = "${local.name_prefix}-gold-schedule"
  description         = "Trigger Gold processing every 5 minutes"
  schedule_expression = "rate(5 minutes)"
  state               = "DISABLED" # Enable via CLI after image is pushed
}

resource "aws_cloudwatch_event_target" "gold_schedule" {
  rule      = aws_cloudwatch_event_rule.gold_schedule.name
  target_id = "gold-processing"
  arn       = aws_sfn_state_machine.gold_processing.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
}

# Data Quality schedule - every 15 minutes
resource "aws_cloudwatch_event_rule" "data_quality_schedule" {
  name                = "${local.name_prefix}-data-quality-schedule"
  description         = "Trigger Data Quality checks every 15 minutes"
  schedule_expression = "rate(15 minutes)"
  state               = "DISABLED" # Enable via CLI after deployment
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
      EMR_APP_ID         = aws_emrserverless_application.spark.id
      EMR_ROLE_ARN       = aws_iam_role.emr_serverless.arn
      S3_BUCKET          = aws_s3_bucket.data.id
      S3_TABLES_ARN      = aws_s3tables_table_bucket.wikistream.arn
      MSK_BOOTSTRAP      = aws_msk_cluster.wikistream.bootstrap_brokers_sasl_iam
      SNS_TOPIC_ARN      = aws_sns_topic.alerts.arn
    }
  }
}

# Lambda code - inline for simplicity
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
        
        # Start new Bronze streaming job
        iceberg_packages = (
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
            "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,"
            "software.amazon.awssdk:bundle:2.29.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "software.amazon.msk:aws-msk-iam-auth:2.2.0"
        )
        
        spark_conf = [
            "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
            "--conf", "spark.sql.adaptive.skewJoin.enabled=true",
            "--conf", "spark.sql.iceberg.handle-timestamp-without-timezone=true"
        ]
        
        spark_conf = (
            f"--conf spark.executor.cores=2 "
            f"--conf spark.executor.memory=4g "
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
                    's3MonitoringConfiguration': {
                        'logUri': f's3://{s3_bucket}/logs/emr/bronze/'
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

output "silver_state_machine_arn" {
  description = "Silver Processing State Machine ARN"
  value       = aws_sfn_state_machine.silver_processing.arn
}

output "gold_state_machine_arn" {
  description = "Gold Processing State Machine ARN"
  value       = aws_sfn_state_machine.gold_processing.arn
}

output "data_quality_state_machine_arn" {
  description = "Data Quality State Machine ARN"
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
