# =============================================================================
# WikiStream Pipeline - Terraform Variables
# =============================================================================

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

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "aws_current_profile" {
  description = "AWS current profile"
  type        = string
  default     = "neuefische"
}

# =============================================================================
# Alerting Configuration
# =============================================================================

variable "alert_email" {
  description = "Email address to receive pipeline alerts and DQ notifications"
  type        = string
  default     = "mrshihabullah@gmail.com"
}
