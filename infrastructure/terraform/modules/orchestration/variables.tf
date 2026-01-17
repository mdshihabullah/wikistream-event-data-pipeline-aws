# =============================================================================
# Orchestration Module - Variables
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

# IAM Roles
variable "step_functions_role_arn" {
  description = "Step Functions role ARN"
  type        = string
}

variable "eventbridge_sfn_role_arn" {
  description = "EventBridge to Step Functions role ARN"
  type        = string
}

variable "emr_serverless_role_arn" {
  description = "EMR Serverless role ARN"
  type        = string
}

# Resource References
variable "emr_serverless_app_id" {
  description = "EMR Serverless application ID"
  type        = string
}

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

# Configuration
variable "job_timeout_seconds" {
  description = "Timeout for EMR jobs (seconds)"
  type        = number
  default     = 900
}

variable "batch_pipeline_wait_seconds" {
  description = "Wait time between batch pipeline cycles (seconds)"
  type        = number
  default     = 600
}

# Spark package versions (for DRY)
variable "spark_packages" {
  description = "Base Spark packages string"
  type        = string
}

variable "spark_packages_with_deequ" {
  description = "Spark packages with Deequ"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
