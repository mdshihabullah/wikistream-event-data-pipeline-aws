# =============================================================================
# Compute Module - Variables
# =============================================================================

variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "account_id" {
  description = "AWS Account ID"
  type        = string
}

variable "region" {
  description = "AWS Region"
  type        = string
}

# Network Configuration
variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnets" {
  description = "List of private subnet IDs"
  type        = list(string)
}

variable "ecs_security_group_id" {
  description = "Security group ID for ECS"
  type        = string
}

variable "emr_security_group_id" {
  description = "Security group ID for EMR"
  type        = string
}

# IAM Roles
variable "ecs_execution_role_arn" {
  description = "ECS task execution role ARN"
  type        = string
}

variable "ecs_task_role_arn" {
  description = "ECS task role ARN"
  type        = string
}

variable "emr_serverless_role_arn" {
  description = "EMR Serverless execution role ARN"
  type        = string
}

variable "bronze_restart_lambda_role_arn" {
  description = "Lambda role ARN for bronze restart"
  type        = string
}

# MSK Configuration
variable "msk_bootstrap_brokers" {
  description = "MSK bootstrap brokers"
  type        = string
}

# Storage Configuration
variable "data_bucket_id" {
  description = "S3 data bucket ID"
  type        = string
}

variable "s3_tables_bucket_arn" {
  description = "S3 Tables bucket ARN"
  type        = string
}

variable "sns_topic_arn" {
  description = "SNS topic ARN for alerts"
  type        = string
}

# ECS Configuration
variable "ecs_cpu" {
  description = "ECS task CPU units"
  type        = string
  default     = "256"
}

variable "ecs_memory" {
  description = "ECS task memory"
  type        = string
  default     = "512"
}

variable "ecs_container_insights" {
  description = "Enable Container Insights"
  type        = bool
  default     = false
}

# EMR Configuration
variable "emr_max_vcpu" {
  description = "Maximum vCPU for EMR application"
  type        = string
  default     = "16 vCPU"
}

variable "emr_max_memory" {
  description = "Maximum memory for EMR application"
  type        = string
  default     = "64 GB"
}

variable "emr_max_disk" {
  description = "Maximum disk for EMR application"
  type        = string
  default     = "200 GB"
}

variable "emr_idle_timeout_minutes" {
  description = "EMR auto-stop idle timeout"
  type        = number
  default     = 15
}

variable "emr_prewarm_driver_count" {
  description = "Number of pre-warmed drivers"
  type        = number
  default     = 1
}

variable "emr_prewarm_executor_count" {
  description = "Number of pre-warmed executors"
  type        = number
  default     = 1
}

# Feature Flags
variable "enable_ecr_scan" {
  description = "Enable ECR image scanning"
  type        = bool
  default     = false
}

variable "log_retention_days" {
  description = "CloudWatch log retention"
  type        = number
  default     = 7
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
