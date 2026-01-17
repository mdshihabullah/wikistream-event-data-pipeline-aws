# =============================================================================
# Monitoring Module - Outputs
# =============================================================================
# Note: SNS topic ARN is passed through from root module

output "emr_serverless_log_group" {
  description = "EMR Serverless CloudWatch log group name"
  value       = aws_cloudwatch_log_group.emr_serverless.name
}

output "cloudwatch_dashboard_name" {
  description = "CloudWatch dashboard name"
  value       = aws_cloudwatch_dashboard.pipeline.dashboard_name
}

output "ecs_cpu_alarm_arn" {
  description = "ECS CPU alarm ARN"
  value       = aws_cloudwatch_metric_alarm.ecs_cpu.arn
}

output "bronze_health_alarm_arn" {
  description = "Bronze health alarm ARN"
  value       = aws_cloudwatch_metric_alarm.bronze_health.arn
}

output "batch_pipeline_failure_alarm_arn" {
  description = "Batch pipeline failure alarm ARN"
  value       = aws_cloudwatch_metric_alarm.batch_pipeline_failure.arn
}
