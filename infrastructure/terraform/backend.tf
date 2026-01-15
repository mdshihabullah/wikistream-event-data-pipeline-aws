# =============================================================================
# WikiStream Pipeline - Terraform Backend Configuration
# =============================================================================
# S3 backend with DynamoDB state locking
# Backend configuration is provided dynamically during terraform init
# This allows the scripts to work with any AWS account/profile
#
# Usage: terraform init -backend-config="bucket=wikistream-terraform-state-<ACCOUNT_ID>" ...
# =============================================================================

terraform {
  backend "s3" {
    # Values provided via -backend-config in create_infra.sh and destroy_all.sh:
    # - bucket         = "wikistream-terraform-state-${AWS_ACCOUNT_ID}"
    # - key            = "wikistream/${ENVIRONMENT}/terraform.tfstate"
    # - region         = "${AWS_REGION}"
    # - dynamodb_table = "wikistream-terraform-locks"
    # - encrypt        = true
  }
}
