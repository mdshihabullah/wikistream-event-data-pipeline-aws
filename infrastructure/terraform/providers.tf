# =============================================================================
# WikiStream Pipeline - Provider Configuration
# =============================================================================

provider "aws" {
  region  = var.aws_region
  profile = var.aws_current_profile

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}
