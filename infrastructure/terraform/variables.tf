# =============================================================================
# WikiStream Pipeline - Terraform Variables
# =============================================================================
# All configurable parameters for the infrastructure
# Environment-specific values should be set in environments/*.tfvars
# =============================================================================

# -----------------------------------------------------------------------------
# Core Configuration
# -----------------------------------------------------------------------------

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "wikistream"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "aws_current_profile" {
  description = "AWS CLI profile to use"
  type        = string
  default     = "neuefische"
}

# -----------------------------------------------------------------------------
# Networking Configuration
# -----------------------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones (2 for dev/cost savings, 3 for prod)"
  type        = number
  default     = 2

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "AZ count must be 2 or 3."
  }
}

variable "single_nat_gateway" {
  description = "Use single NAT gateway (cost savings for non-prod)"
  type        = bool
  default     = true
}

# -----------------------------------------------------------------------------
# MSK (Kafka) Configuration
# -----------------------------------------------------------------------------

variable "msk_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.t3.small"
}

variable "msk_broker_count" {
  description = "Number of MSK brokers"
  type        = number
  default     = 2
}

variable "msk_storage_size" {
  description = "EBS storage size per broker (GB)"
  type        = number
  default     = 50
}

variable "kafka_retention_hours" {
  description = "Kafka log retention in hours"
  type        = number
  default     = 168 # 7 days
}

# -----------------------------------------------------------------------------
# EMR Serverless Configuration
# -----------------------------------------------------------------------------

variable "emr_max_vcpu" {
  description = "Maximum vCPU for EMR Serverless application"
  type        = string
  default     = "16 vCPU"
}

variable "emr_max_memory" {
  description = "Maximum memory for EMR Serverless application"
  type        = string
  default     = "64 GB"
}

variable "emr_max_disk" {
  description = "Maximum disk for EMR Serverless application"
  type        = string
  default     = "200 GB"
}

variable "emr_idle_timeout_minutes" {
  description = "EMR auto-stop idle timeout in minutes"
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

# -----------------------------------------------------------------------------
# ECS Configuration
# -----------------------------------------------------------------------------

variable "ecs_cpu" {
  description = "ECS task CPU units (256 = 0.25 vCPU)"
  type        = string
  default     = "256"
}

variable "ecs_memory" {
  description = "ECS task memory (MB)"
  type        = string
  default     = "512"
}

variable "ecs_container_insights" {
  description = "Enable ECS Container Insights (cost consideration)"
  type        = bool
  default     = false
}

# -----------------------------------------------------------------------------
# S3 Tables Configuration
# -----------------------------------------------------------------------------

variable "iceberg_compaction_target_mb" {
  description = "Target file size for Iceberg compaction (MB)"
  type        = number
  default     = 512
}

variable "iceberg_snapshot_retention_hours" {
  description = "Maximum snapshot age in hours"
  type        = number
  default     = 48 # 2 days for dev
}

variable "iceberg_min_snapshots" {
  description = "Minimum snapshots to keep"
  type        = number
  default     = 1
}

variable "iceberg_unreferenced_days" {
  description = "Days before cleaning unreferenced files"
  type        = number
  default     = 3
}

# -----------------------------------------------------------------------------
# Monitoring Configuration
# -----------------------------------------------------------------------------

variable "alert_email" {
  description = "Email address to receive pipeline alerts and DQ notifications"
  type        = string
  default     = "mrshihabullah@gmail.com"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "emr_log_retention_days" {
  description = "EMR Serverless CloudWatch log retention in days"
  type        = number
  default     = 14
}

# -----------------------------------------------------------------------------
# QuickSight (Analytics) Configuration
# -----------------------------------------------------------------------------

variable "quicksight_enable_subscription" {
  description = "Enable QuickSight account subscription creation"
  type        = bool
  default     = false
}

variable "quicksight_account_name" {
  description = "QuickSight account name (subscription)"
  type        = string
  default     = "ne-shihab"
}

variable "quicksight_admin_email" {
  description = "QuickSight admin email (subscription)"
  type        = string
  default     = "shihabullah@outlook.com"
}

variable "quicksight_edition" {
  description = "QuickSight edition (STANDARD or ENTERPRISE)"
  type        = string
  default     = "ENTERPRISE"
}

variable "quicksight_authentication_method" {
  description = "QuickSight authentication method"
  type        = string
  default     = "IAM_AND_QUICKSIGHT"
}

variable "quicksight_namespace" {
  description = "QuickSight namespace"
  type        = string
  default     = "default"
}

variable "quicksight_user_name" {
  description = "QuickSight username for permissions"
  type        = string
  default     = "160884803380"
}

variable "quicksight_athena_workgroup" {
  description = "Athena workgroup for QuickSight"
  type        = string
  default     = "primary"
}

variable "quicksight_import_mode" {
  description = "QuickSight dataset import mode (DIRECT_QUERY or SPICE)"
  type        = string
  default     = "DIRECT_QUERY"

  validation {
    condition     = contains(["DIRECT_QUERY", "SPICE"], var.quicksight_import_mode)
    error_message = "quicksight_import_mode must be DIRECT_QUERY or SPICE."
  }
}

variable "quicksight_service_role_name" {
  description = "QuickSight service role name"
  type        = string
  default     = "aws-quicksight-service-role-v0"
}

variable "quicksight_enable_lakeformation_permissions" {
  description = "Grant Lake Formation permissions for QuickSight"
  type        = bool
  default     = true
}

variable "athena_principal_arns" {
  description = "IAM principal ARNs to grant Athena access via Lake Formation"
  type        = list(string)
  default     = ["arn:aws:iam::160884803380:user/shihab-ne"]
}

# -----------------------------------------------------------------------------
# Step Functions Configuration
# -----------------------------------------------------------------------------

variable "step_function_job_timeout" {
  description = "Timeout for EMR jobs in Step Functions (seconds)"
  type        = number
  default     = 900 # 15 minutes
}

variable "batch_pipeline_wait_seconds" {
  description = "Wait time between batch pipeline cycles (seconds)"
  type        = number
  default     = 600 # 10 minutes
}

# -----------------------------------------------------------------------------
# Feature Flags
# -----------------------------------------------------------------------------

variable "enable_vpc_flow_logs" {
  description = "Enable VPC flow logs (cost consideration)"
  type        = bool
  default     = false
}

variable "enable_ecr_scan" {
  description = "Enable ECR image scanning on push"
  type        = bool
  default     = false
}

variable "force_destroy_s3" {
  description = "Allow Terraform to delete S3 bucket with objects (for dev cleanup)"
  type        = bool
  default     = true
}
