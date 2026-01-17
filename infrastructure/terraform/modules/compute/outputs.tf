# =============================================================================
# Compute Module - Outputs
# =============================================================================

# ECR
output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.producer.repository_url
}

output "ecr_repository_arn" {
  description = "ECR repository ARN"
  value       = aws_ecr_repository.producer.arn
}

# ECS
output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  value       = aws_ecs_cluster.main.arn
}

output "ecs_service_name" {
  description = "ECS service name"
  value       = aws_ecs_service.producer.name
}

output "ecs_log_group_name" {
  description = "ECS CloudWatch log group name"
  value       = aws_cloudwatch_log_group.ecs.name
}

# EMR Serverless
output "emr_serverless_app_id" {
  description = "EMR Serverless application ID"
  value       = aws_emrserverless_application.spark.id
}

output "emr_serverless_app_arn" {
  description = "EMR Serverless application ARN"
  value       = aws_emrserverless_application.spark.arn
}

# Lambda
output "bronze_restart_lambda_arn" {
  description = "Bronze restart Lambda ARN"
  value       = aws_lambda_function.bronze_restart.arn
}

output "bronze_restart_lambda_name" {
  description = "Bronze restart Lambda function name"
  value       = aws_lambda_function.bronze_restart.function_name
}
