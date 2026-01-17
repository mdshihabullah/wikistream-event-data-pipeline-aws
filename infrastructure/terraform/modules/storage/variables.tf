# =============================================================================
# Storage Module - Variables
# =============================================================================

variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "account_id" {
  description = "AWS Account ID"
  type        = string
}

variable "force_destroy" {
  description = "Allow S3 bucket deletion with objects"
  type        = bool
  default     = true
}

# S3 Tables / Iceberg Configuration
variable "iceberg_compaction_target_mb" {
  description = "Target file size for Iceberg compaction"
  type        = number
  default     = 512
}

variable "iceberg_snapshot_retention_hours" {
  description = "Maximum snapshot age in hours"
  type        = number
  default     = 48
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

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
