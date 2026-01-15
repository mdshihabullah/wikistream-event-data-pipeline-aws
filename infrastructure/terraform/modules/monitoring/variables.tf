# =============================================================================
# Monitoring Module - Variables
# =============================================================================

variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "region" {
  description = "AWS Region"
  type        = string
}

variable "account_id" {
  description = "AWS Account ID"
  type        = string
}

# Resource References for monitoring
variable "ecs_cluster_name" {
  description = "ECS cluster name"
  type        = string
}

variable "ecs_service_name" {
  description = "ECS service name"
  type        = string
}

variable "msk_cluster_name" {
  description = "MSK cluster name"
  type        = string
}

variable "emr_serverless_app_id" {
  description = "EMR Serverless application ID"
  type        = string
}

variable "batch_pipeline_state_machine_arn" {
  description = "Batch pipeline state machine ARN"
  type        = string
}

variable "bronze_restart_lambda_arn" {
  description = "Bronze restart Lambda ARN"
  type        = string
}

variable "bronze_restart_lambda_name" {
  description = "Bronze restart Lambda function name"
  type        = string
}

# Configuration
variable "alert_email" {
  description = "Email for alerts (not used - SNS created in root)"
  type        = string
  default     = ""
}

variable "sns_topic_arn" {
  description = "SNS topic ARN for alerts (created in root module)"
  type        = string
}

variable "log_retention_days" {
  description = "EMR log retention days"
  type        = number
  default     = 14
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
