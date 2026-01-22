# =============================================================================
# Monitoring Module - CloudWatch, Alarms, Dashboard
# =============================================================================
# Note: SNS topic is created in root module to avoid circular dependencies

# -----------------------------------------------------------------------------
# CloudWatch Log Group for EMR Serverless
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "emr_serverless" {
  name              = "/aws/emr-serverless/${var.name_prefix}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms
# -----------------------------------------------------------------------------

# ECS CPU Alarm
resource "aws_cloudwatch_metric_alarm" "ecs_cpu" {
  alarm_name          = "${var.name_prefix}-producer-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ECS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_actions       = [var.sns_topic_arn]

  dimensions = {
    ClusterName = var.ecs_cluster_name
    ServiceName = var.ecs_service_name
  }

  tags = var.tags
}

# Bronze Job Health Alarm
resource "aws_cloudwatch_metric_alarm" "bronze_health" {
  alarm_name          = "${var.name_prefix}-bronze-health"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "BronzeRecordsProcessed"
  namespace           = "WikiStream/Pipeline"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Bronze streaming job has stopped processing records - triggering auto-restart"
  alarm_actions       = [var.sns_topic_arn, var.bronze_restart_lambda_arn]
  ok_actions          = [var.sns_topic_arn]
  treat_missing_data  = "breaching"

  dimensions = {
    Layer = "bronze"
  }

  tags = var.tags
}

# Batch Pipeline Failure Alarm
resource "aws_cloudwatch_metric_alarm" "batch_pipeline_failure" {
  alarm_name          = "${var.name_prefix}-batch-pipeline-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Batch pipeline (Silver→DQ→Gold) has failed"
  alarm_actions       = [var.sns_topic_arn]
  ok_actions          = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = var.batch_pipeline_state_machine_arn
  }

  tags = var.tags
}

# DLQ High Rate Alarm
resource "aws_cloudwatch_metric_alarm" "dlq_high_rate" {
  alarm_name          = "${var.name_prefix}-dlq-high-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "DLQMessagesProduced"
  namespace           = "WikiStream/Producer"
  period              = 60
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "High rate of DLQ messages - many malformed events from Wikipedia SSE"
  alarm_actions       = [var.sns_topic_arn]
  ok_actions          = [var.sns_topic_arn]
  treat_missing_data  = "notBreaching"

  tags = var.tags
}

# -----------------------------------------------------------------------------
# Lambda Permission for CloudWatch
# -----------------------------------------------------------------------------

resource "aws_lambda_permission" "cloudwatch_bronze_restart" {
  statement_id  = "AllowCloudWatchInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.bronze_restart_lambda_name
  principal     = "lambda.alarms.cloudwatch.amazonaws.com"
  source_arn    = aws_cloudwatch_metric_alarm.bronze_health.arn
}

# -----------------------------------------------------------------------------
# CloudWatch Dashboard
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${var.name_prefix}-pipeline-dashboard"

  dashboard_body = templatefile("${path.module}/templates/dashboard.json.tftpl", {
    region               = var.region
    name_prefix          = var.name_prefix
    ecs_cluster_name     = var.ecs_cluster_name
    ecs_service_name     = var.ecs_service_name
    msk_cluster_name     = var.msk_cluster_name
    emr_app_id           = var.emr_serverless_app_id
    batch_pipeline_arn   = var.batch_pipeline_state_machine_arn
    ecs_cpu_alarm_arn    = aws_cloudwatch_metric_alarm.ecs_cpu.arn
    bronze_health_arn    = aws_cloudwatch_metric_alarm.bronze_health.arn
    pipeline_failure_arn = aws_cloudwatch_metric_alarm.batch_pipeline_failure.arn
    dlq_alarm_arn        = aws_cloudwatch_metric_alarm.dlq_high_rate.arn
  })
}
