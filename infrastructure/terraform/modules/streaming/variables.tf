# =============================================================================
# Streaming Module - Variables
# =============================================================================

variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnets" {
  description = "List of private subnet IDs"
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group ID for MSK"
  type        = string
}

# MSK Configuration
variable "instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.t3.small"
}

variable "broker_count" {
  description = "Number of MSK brokers"
  type        = number
  default     = 2
}

variable "storage_size" {
  description = "EBS storage size per broker (GB)"
  type        = number
  default     = 50
}

variable "retention_hours" {
  description = "Kafka log retention in hours"
  type        = number
  default     = 168
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
