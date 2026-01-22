# =============================================================================
# Compute Module - ECS, EMR Serverless, ECR, Lambda
# =============================================================================

# -----------------------------------------------------------------------------
# ECR Repository
# -----------------------------------------------------------------------------

resource "aws_ecr_repository" "producer" {
  name                 = "${var.name_prefix}-producer"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = var.enable_ecr_scan
  }

  tags = var.tags
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

# -----------------------------------------------------------------------------
# ECS Cluster
# -----------------------------------------------------------------------------

resource "aws_ecs_cluster" "main" {
  name = "${var.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = var.ecs_container_insights ? "enabled" : "disabled"
  }

  tags = var.tags
}

# -----------------------------------------------------------------------------
# ECS CloudWatch Log Group
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/ecs/${var.name_prefix}-producer"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# -----------------------------------------------------------------------------
# ECS Task Definition
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "producer" {
  family                   = "${var.name_prefix}-producer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.ecs_cpu
  memory                   = var.ecs_memory
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([{
    name  = "producer"
    image = "${aws_ecr_repository.producer.repository_url}:latest"

    environment = [
      { name = "KAFKA_BOOTSTRAP_SERVERS", value = var.msk_bootstrap_brokers },
      { name = "ENVIRONMENT", value = var.environment },
      { name = "AWS_REGION", value = var.region }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.ecs.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "producer"
      }
    }

    # Note: healthCheck removed - Chainguard minimal image has no shell
    # ECS monitors container health via task running state
    # Application health is monitored via CloudWatch logs and metrics
  }])

  tags = var.tags
}

# -----------------------------------------------------------------------------
# ECS Service
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "producer" {
  name            = "${var.name_prefix}-producer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.producer.arn
  desired_count   = 0 # Start with 0, enable after image is pushed
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnets
    security_groups  = [var.ecs_security_group_id]
    assign_public_ip = false
  }

  # Ignore desired_count changes (managed via CLI/console)
  lifecycle {
    ignore_changes = [desired_count, task_definition]
  }

  depends_on = [aws_ecr_repository.producer]

  tags = var.tags
}

# -----------------------------------------------------------------------------
# EMR Serverless Application
# -----------------------------------------------------------------------------

resource "aws_emrserverless_application" "spark" {
  name          = "${var.name_prefix}-spark"
  release_label = "emr-7.12.0"
  type          = "SPARK"

  maximum_capacity {
    cpu    = var.emr_max_vcpu
    memory = var.emr_max_memory
    disk   = var.emr_max_disk
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.emr_idle_timeout_minutes
  }

  network_configuration {
    subnet_ids         = var.private_subnets
    security_group_ids = [var.emr_security_group_id]
  }

  # Pre-warm driver (only if count > 0)
  dynamic "initial_capacity" {
    for_each = var.emr_prewarm_driver_count > 0 ? [1] : []
    content {
      initial_capacity_type = "DRIVER"
      initial_capacity_config {
        worker_count = var.emr_prewarm_driver_count
        worker_configuration {
          cpu    = "2 vCPU"
          memory = "4 GB"
          disk   = "20 GB"
        }
      }
    }
  }

  # Pre-warm executor (only if count > 0)
  dynamic "initial_capacity" {
    for_each = var.emr_prewarm_executor_count > 0 ? [1] : []
    content {
      initial_capacity_type = "EXECUTOR"
      initial_capacity_config {
        worker_count = var.emr_prewarm_executor_count
        worker_configuration {
          cpu    = "2 vCPU"
          memory = "4 GB"
          disk   = "20 GB"
        }
      }
    }
  }

  tags = var.tags
}

# -----------------------------------------------------------------------------
# Bronze Restart Lambda
# -----------------------------------------------------------------------------

data "archive_file" "bronze_restart_lambda" {
  type        = "zip"
  output_path = "${path.module}/bronze_restart_lambda.zip"

  source {
    content = templatefile("${path.module}/templates/bronze_restart_lambda.py.tftpl", {
      name_prefix = var.name_prefix
    })
    filename = "index.py"
  }
}

resource "aws_lambda_function" "bronze_restart" {
  function_name = "${var.name_prefix}-bronze-restart"
  role          = var.bronze_restart_lambda_role_arn
  handler       = "index.handler"
  runtime       = "python3.12"
  timeout       = 60
  memory_size   = 128

  filename         = data.archive_file.bronze_restart_lambda.output_path
  source_code_hash = data.archive_file.bronze_restart_lambda.output_base64sha256

  environment {
    variables = {
      EMR_APP_ID    = aws_emrserverless_application.spark.id
      EMR_ROLE_ARN  = var.emr_serverless_role_arn
      S3_BUCKET     = var.data_bucket_id
      S3_TABLES_ARN = var.s3_tables_bucket_arn
      MSK_BOOTSTRAP = var.msk_bootstrap_brokers
      SNS_TOPIC_ARN = var.sns_topic_arn
    }
  }

  tags = var.tags
}
