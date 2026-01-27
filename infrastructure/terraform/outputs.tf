# =============================================================================
# WikiStream Pipeline - Outputs
# =============================================================================
# All outputs required by create_infra.sh and destroy_all.sh scripts
# IMPORTANT: Do not rename these outputs - scripts depend on exact names
# =============================================================================

# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------

output "vpc_id" {
  description = "VPC ID"
  value       = module.networking.vpc_id
}

# -----------------------------------------------------------------------------
# Streaming (MSK)
# -----------------------------------------------------------------------------

output "msk_bootstrap_brokers_iam" {
  description = "MSK Bootstrap Brokers (IAM)"
  value       = module.streaming.bootstrap_brokers_iam
  sensitive   = true
}

output "msk_cluster_arn" {
  description = "MSK Cluster ARN"
  value       = module.streaming.cluster_arn
}

# -----------------------------------------------------------------------------
# Storage
# -----------------------------------------------------------------------------

output "s3_tables_bucket_arn" {
  description = "S3 Tables Bucket ARN"
  value       = module.storage.s3_tables_bucket_arn
}

output "data_bucket" {
  description = "S3 Data Bucket"
  value       = module.storage.data_bucket_id
}

# -----------------------------------------------------------------------------
# Compute
# -----------------------------------------------------------------------------

output "emr_serverless_app_id" {
  description = "EMR Serverless Application ID"
  value       = module.compute.emr_serverless_app_id
}

output "ecr_repository_url" {
  description = "ECR Repository URL for Producer"
  value       = module.compute.ecr_repository_url
}

output "ecs_cluster_name" {
  description = "ECS Cluster Name"
  value       = module.compute.ecs_cluster_name
}

output "bronze_restart_lambda_arn" {
  description = "Bronze Job Auto-Restart Lambda ARN"
  value       = module.compute.bronze_restart_lambda_arn
}

# -----------------------------------------------------------------------------
# IAM
# -----------------------------------------------------------------------------

output "emr_serverless_role_arn" {
  description = "EMR Serverless Execution Role ARN"
  value       = aws_iam_role.emr_serverless.arn
}

# -----------------------------------------------------------------------------
# Orchestration
# -----------------------------------------------------------------------------

output "batch_pipeline_state_machine_arn" {
  description = "Unified Batch Pipeline State Machine ARN (Silver→DQ→Gold)"
  value       = module.orchestration.batch_pipeline_state_machine_arn
}

output "silver_state_machine_arn" {
  description = "Silver Processing State Machine ARN (standalone)"
  value       = module.orchestration.silver_state_machine_arn
}

output "gold_state_machine_arn" {
  description = "Gold Processing State Machine ARN (standalone)"
  value       = module.orchestration.gold_state_machine_arn
}

output "data_quality_state_machine_arn" {
  description = "Data Quality State Machine ARN (standalone)"
  value       = module.orchestration.data_quality_state_machine_arn
}

# -----------------------------------------------------------------------------
# Monitoring
# -----------------------------------------------------------------------------

output "alerts_sns_topic_arn" {
  description = "SNS Topic ARN for alerts"
  value       = aws_sns_topic.alerts.arn
}

output "cloudwatch_dashboard_name" {
  description = "CloudWatch Dashboard name for pipeline monitoring"
  value       = module.monitoring.cloudwatch_dashboard_name
}

output "emr_serverless_log_group" {
  description = "CloudWatch Log Group for EMR Serverless jobs"
  value       = module.monitoring.emr_serverless_log_group
}

# -----------------------------------------------------------------------------
# Analytics (QuickSight)
# -----------------------------------------------------------------------------

output "quicksight_data_source_arn" {
  description = "QuickSight Athena data source ARN"
  value       = module.analytics.quicksight_data_source_arn
}

output "quicksight_hourly_stats_dataset_arn" {
  description = "QuickSight dataset ARN for hourly_stats"
  value       = module.analytics.quicksight_hourly_stats_dataset_arn
}

output "quicksight_risk_scores_dataset_arn" {
  description = "QuickSight dataset ARN for risk_scores"
  value       = module.analytics.quicksight_risk_scores_dataset_arn
}

output "quicksight_daily_summary_dataset_arn" {
  description = "QuickSight dataset ARN for daily_analytics_summary"
  value       = module.analytics.quicksight_daily_summary_dataset_arn
}
