variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "account_id" {
  description = "AWS Account ID"
  type        = string
}

variable "region" {
  description = "AWS region for resources"
  type        = string
}

variable "s3_tables_bucket_name" {
  description = "S3 Tables bucket name"
  type        = string
}

variable "s3_tables_bucket_arn" {
  description = "S3 Tables bucket ARN"
  type        = string
}

variable "athena_principal_arns" {
  description = "Additional IAM principal ARNs for Athena Lake Formation grants"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

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
