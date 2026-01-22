# =============================================================================
# WikiStream Pipeline - Root Module
# =============================================================================
# Orchestrates all child modules to create the complete infrastructure
#
# Architecture Order (avoiding circular dependencies):
#   1. Networking → Storage → Streaming
#   2. SNS Topic (created here - needed by multiple modules)
#   3. Compute (needs SNS)
#   4. IAM (needs SNS, MSK, S3, EMR)
#   5. Orchestration (needs IAM, Compute)
#   6. Monitoring (needs all above)
#
# Usage:
#   terraform init -backend-config="bucket=wikistream-terraform-state-<ACCOUNT_ID>" ...
#   terraform apply -var-file=environments/dev.tfvars
# =============================================================================

# -----------------------------------------------------------------------------
# SNS Topic (Created early - needed by multiple modules)
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name = "${local.name_prefix}-alerts"
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "alert_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# -----------------------------------------------------------------------------
# Networking Module
# -----------------------------------------------------------------------------

module "networking" {
  source = "./modules/networking"

  name_prefix        = local.name_prefix
  vpc_cidr           = var.vpc_cidr
  azs                = local.azs
  single_nat_gateway = var.single_nat_gateway
  enable_flow_logs   = var.enable_vpc_flow_logs
  tags               = local.common_tags
}

# -----------------------------------------------------------------------------
# Storage Module
# -----------------------------------------------------------------------------

module "storage" {
  source = "./modules/storage"

  name_prefix                      = local.name_prefix
  account_id                       = local.account_id
  force_destroy                    = var.force_destroy_s3
  iceberg_compaction_target_mb     = var.iceberg_compaction_target_mb
  iceberg_snapshot_retention_hours = var.iceberg_snapshot_retention_hours
  iceberg_min_snapshots            = var.iceberg_min_snapshots
  iceberg_unreferenced_days        = var.iceberg_unreferenced_days
  tags                             = local.common_tags
}

# -----------------------------------------------------------------------------
# Streaming Module
# -----------------------------------------------------------------------------

module "streaming" {
  source = "./modules/streaming"

  name_prefix        = local.name_prefix
  vpc_id             = module.networking.vpc_id
  private_subnets    = module.networking.private_subnets
  security_group_id  = module.networking.msk_security_group_id
  instance_type      = var.msk_instance_type
  broker_count       = var.msk_broker_count
  storage_size       = var.msk_storage_size
  retention_hours    = var.kafka_retention_hours
  log_retention_days = var.log_retention_days
  tags               = local.common_tags
}

# -----------------------------------------------------------------------------
# Compute Module (No IAM dependency - uses placeholder, updated after IAM)
# -----------------------------------------------------------------------------

module "compute" {
  source = "./modules/compute"

  name_prefix                    = local.name_prefix
  account_id                     = local.account_id
  region                         = local.region
  environment                    = var.environment
  vpc_id                         = module.networking.vpc_id
  private_subnets                = module.networking.private_subnets
  ecs_security_group_id          = module.networking.ecs_security_group_id
  emr_security_group_id          = module.networking.emr_security_group_id
  ecs_execution_role_arn         = aws_iam_role.ecs_execution.arn
  ecs_task_role_arn              = aws_iam_role.ecs_task.arn
  emr_serverless_role_arn        = aws_iam_role.emr_serverless.arn
  bronze_restart_lambda_role_arn = aws_iam_role.bronze_restart_lambda.arn
  msk_bootstrap_brokers          = module.streaming.bootstrap_brokers_iam
  data_bucket_id                 = module.storage.data_bucket_id
  s3_tables_bucket_arn           = module.storage.s3_tables_bucket_arn
  sns_topic_arn                  = aws_sns_topic.alerts.arn
  ecs_cpu                        = var.ecs_cpu
  ecs_memory                     = var.ecs_memory
  ecs_container_insights         = var.ecs_container_insights
  emr_max_vcpu                   = var.emr_max_vcpu
  emr_max_memory                 = var.emr_max_memory
  emr_max_disk                   = var.emr_max_disk
  emr_idle_timeout_minutes       = var.emr_idle_timeout_minutes
  emr_prewarm_driver_count       = var.emr_prewarm_driver_count
  emr_prewarm_executor_count     = var.emr_prewarm_executor_count
  enable_ecr_scan                = var.enable_ecr_scan
  log_retention_days             = var.log_retention_days
  tags                           = local.common_tags

  depends_on = [
    aws_iam_role.ecs_execution,
    aws_iam_role.ecs_task,
    aws_iam_role.emr_serverless,
    aws_iam_role.bronze_restart_lambda
  ]
}

# -----------------------------------------------------------------------------
# IAM Roles (Inline - to break circular dependencies)
# -----------------------------------------------------------------------------

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
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ECS Task Role
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
  tags = local.common_tags
}

resource "aws_iam_role_policy" "ecs_task_kafka" {
  name = "${local.name_prefix}-ecs-kafka-policy"
  role = aws_iam_role.ecs_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:AlterCluster"]
        Resource = module.streaming.cluster_arn
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
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "WikiStream/Producer"
          }
        }
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
  tags = local.common_tags
}

resource "aws_iam_role_policy" "emr_serverless" {
  name = "${local.name_prefix}-emr-serverless-policy"
  role = aws_iam_role.emr_serverless.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3DataBucketAccess"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [module.storage.data_bucket_arn, "${module.storage.data_bucket_arn}/*"]
      },
      { Sid = "S3TablesFullAccess", Effect = "Allow", Action = ["s3tables:*"], Resource = "*" },
      {
        Sid      = "S3TablesDataAccess"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = ["arn:aws:s3:::*", "arn:aws:s3:::*/*"]
      },
      {
        Sid      = "S3ExpressDataAccess"
        Effect   = "Allow"
        Action   = ["s3express:CreateSession", "s3express:GetObject", "s3express:PutObject", "s3express:DeleteObject", "s3express:ListBucket"]
        Resource = "*"
      },
      {
        Sid      = "S3TablesUnderlyingStorage"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts", "s3:HeadObject", "s3:HeadBucket"]
        Resource = ["arn:aws:s3:::s3tables-*", "arn:aws:s3:::s3tables-*/*", "arn:aws:s3:::*--table-s3", "arn:aws:s3:::*--table-s3/*"]
      },
      {
        Sid      = "MSKAccess"
        Effect   = "Allow"
        Action   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:*Topic*", "kafka-cluster:ReadData", "kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
        Resource = "*"
      },
      {
        Sid      = "GlueCatalogAccess"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions", "glue:CreateDatabase", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:BatchCreatePartition", "glue:BatchDeletePartition"]
        Resource = ["arn:aws:glue:${local.region}:${local.account_id}:catalog", "arn:aws:glue:${local.region}:${local.account_id}:database/*", "arn:aws:glue:${local.region}:${local.account_id}:table/*"]
      },
      { Sid = "LakeFormationAccess", Effect = "Allow", Action = ["lakeformation:GetDataAccess"], Resource = "*" },
      { Sid = "CloudWatchMetrics", Effect = "Allow", Action = ["cloudwatch:PutMetricData"], Resource = "*" },
      {
        Sid      = "CloudWatchLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
        Resource = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/emr-serverless/*", "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/emr-serverless/*:log-stream:*"]
      },
      { Sid = "CloudWatchLogsDescribe", Effect = "Allow", Action = ["logs:DescribeLogGroups"], Resource = "*" },
      { Sid = "SNSPublish", Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.alerts.arn }
    ]
  })
}

# Step Functions Role
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
  tags = local.common_tags
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${local.name_prefix}-sfn-policy"
  role = aws_iam_role.step_functions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "EMRServerlessJobManagement"
        Effect   = "Allow"
        Action   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun", "emr-serverless:TagResource"]
        Resource = [module.compute.emr_serverless_app_arn, "${module.compute.emr_serverless_app_arn}/jobruns/*"]
      },
      {
        Sid       = "PassRoleToEMR"
        Effect    = "Allow"
        Action    = ["iam:PassRole"]
        Resource  = aws_iam_role.emr_serverless.arn
        Condition = { StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
      },
      {
        Sid      = "SelfTrigger"
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = "arn:aws:states:${local.region}:${local.account_id}:stateMachine:${local.name_prefix}-batch-pipeline"
      },
      { Sid = "SNSPublish", Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.alerts.arn },
      {
        Sid      = "CloudWatchLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"]
        Resource = "*"
      },
      {
        Sid      = "XRayAccess"
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
        Resource = "*"
      },
      {
        Sid      = "EventBridgeManagedRules"
        Effect   = "Allow"
        Action   = ["events:PutTargets", "events:PutRule", "events:DescribeRule", "events:DeleteRule", "events:RemoveTargets"]
        Resource = ["arn:aws:events:${local.region}:${local.account_id}:rule/StepFunctionsGetEventsForEMRServerlessJobRunRule", "arn:aws:events:${local.region}:${local.account_id}:rule/StepFunctions*"]
      },
      { Sid = "CloudWatchMetrics", Effect = "Allow", Action = ["cloudwatch:PutMetricData"], Resource = "*" }
    ]
  })
}

# EventBridge Role
resource "aws_iam_role" "eventbridge_sfn" {
  name = "${local.name_prefix}-eventbridge-sfn-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = ["events.amazonaws.com", "scheduler.amazonaws.com"] }
    }]
  })
  tags = local.common_tags
}

# Lambda Role (Bronze Restart)
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
  tags = local.common_tags
}

resource "aws_iam_role_policy" "bronze_restart_lambda" {
  name = "${local.name_prefix}-bronze-restart-lambda-policy"
  role = aws_iam_role.bronze_restart_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "EMRServerlessAccess"
        Effect   = "Allow"
        Action   = ["emr-serverless:ListJobRuns", "emr-serverless:StartJobRun", "emr-serverless:GetJobRun"]
        Resource = [module.compute.emr_serverless_app_arn, "${module.compute.emr_serverless_app_arn}/jobruns/*"]
      },
      {
        Sid       = "PassRoleToEMR"
        Effect    = "Allow"
        Action    = ["iam:PassRole"]
        Resource  = aws_iam_role.emr_serverless.arn
        Condition = { StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
      },
      { Sid = "SNSPublish", Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.alerts.arn },
      {
        Sid      = "CloudWatchLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${local.region}:${local.account_id}:*"
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Orchestration Module
# -----------------------------------------------------------------------------

module "orchestration" {
  source = "./modules/orchestration"

  name_prefix                 = local.name_prefix
  account_id                  = local.account_id
  region                      = local.region
  step_functions_role_arn     = aws_iam_role.step_functions.arn
  eventbridge_sfn_role_arn    = aws_iam_role.eventbridge_sfn.arn
  emr_serverless_role_arn     = aws_iam_role.emr_serverless.arn
  emr_serverless_app_id       = module.compute.emr_serverless_app_id
  data_bucket_id              = module.storage.data_bucket_id
  s3_tables_bucket_arn        = module.storage.s3_tables_bucket_arn
  sns_topic_arn               = aws_sns_topic.alerts.arn
  job_timeout_seconds         = var.step_function_job_timeout
  batch_pipeline_wait_seconds = var.batch_pipeline_wait_seconds
  spark_packages              = local.spark_packages
  spark_packages_with_deequ   = local.spark_packages_with_deequ
  tags                        = local.common_tags

  depends_on = [module.compute, aws_iam_role.step_functions]
}

# -----------------------------------------------------------------------------
# Monitoring Module
# -----------------------------------------------------------------------------

module "monitoring" {
  source = "./modules/monitoring"

  name_prefix                      = local.name_prefix
  region                           = local.region
  account_id                       = local.account_id
  ecs_cluster_name                 = module.compute.ecs_cluster_name
  ecs_service_name                 = module.compute.ecs_service_name
  msk_cluster_name                 = module.streaming.cluster_name
  emr_serverless_app_id            = module.compute.emr_serverless_app_id
  batch_pipeline_state_machine_arn = module.orchestration.batch_pipeline_state_machine_arn
  bronze_restart_lambda_arn        = module.compute.bronze_restart_lambda_arn
  bronze_restart_lambda_name       = module.compute.bronze_restart_lambda_name
  sns_topic_arn                    = aws_sns_topic.alerts.arn
  log_retention_days               = var.emr_log_retention_days
  tags                             = local.common_tags

  depends_on = [module.compute, module.orchestration]
}

# -----------------------------------------------------------------------------
# S3 Tables Bucket Policy
# -----------------------------------------------------------------------------

resource "time_sleep" "wait_for_iam" {
  depends_on      = [aws_iam_role.emr_serverless, aws_iam_role_policy.emr_serverless]
  create_duration = "30s"
}

resource "aws_s3tables_table_bucket_policy" "wikistream" {
  table_bucket_arn = module.storage.s3_tables_bucket_arn
  resource_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowAccountAccess"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
      Action    = ["s3tables:*"]
      Resource  = [module.storage.s3_tables_bucket_arn, "${module.storage.s3_tables_bucket_arn}/*"]
    }]
  })
  depends_on = [time_sleep.wait_for_iam]
}
