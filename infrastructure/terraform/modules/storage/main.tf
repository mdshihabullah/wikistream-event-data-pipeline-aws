# =============================================================================
# Storage Module - S3 Buckets and S3 Tables
# =============================================================================
# Creates data storage infrastructure for the pipeline
# =============================================================================

# -----------------------------------------------------------------------------
# S3 Data Bucket
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  bucket = "${var.name_prefix}-data-${var.account_id}"

  # Allow Terraform to delete bucket even with objects (for clean destroy)
  force_destroy = var.force_destroy

  tags = var.tags
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "intelligent-tiering"
    status = "Enabled"

    filter {} # Required: empty filter applies to all objects

    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  rule {
    id     = "cleanup-old-checkpoints"
    status = "Enabled"
    filter {
      prefix = "checkpoints/"
    }
    expiration {
      days = 7
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# S3 Tables Bucket (Apache Iceberg)
# -----------------------------------------------------------------------------
# Based on AWS best practices for S3 Tables:
# - Compaction: Combines small files into larger ones (improves query performance)
# - Snapshot Management: Controls snapshot retention (reduces storage costs)
# - Unreferenced File Removal: Cleans up orphaned files (prevents cost accumulation)
# Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance.html
# -----------------------------------------------------------------------------

resource "aws_s3tables_table_bucket" "wikistream" {
  name = "${var.name_prefix}-tables"

  # Encryption Configuration (explicitly defined to prevent provider inconsistency)
  encryption_configuration = {
    sse_algorithm = "AES256"
    kms_key_arn   = null
  }

  maintenance_configuration = {
    # Compaction: Merges small Parquet files into larger ones
    iceberg_compaction = {
      settings = {
        target_file_size_mb = var.iceberg_compaction_target_mb
      }
      status = "enabled"
    }

    # Snapshot Management: Controls how many snapshots to retain
    iceberg_snapshot_management = {
      settings = {
        min_snapshots_to_keep  = var.iceberg_min_snapshots
        max_snapshot_age_hours = var.iceberg_snapshot_retention_hours
      }
      status = "enabled"
    }

    # Unreferenced File Removal: Deletes orphaned files not referenced by any snapshot
    iceberg_unreferenced_file_removal = {
      settings = {
        unreferenced_days = var.iceberg_unreferenced_days
        non_current_days  = 1
      }
      status = "enabled"
    }
  }
}

# -----------------------------------------------------------------------------
# S3 Tables Namespaces (Medallion Architecture)
# -----------------------------------------------------------------------------

resource "aws_s3tables_namespace" "bronze" {
  namespace        = "bronze"
  table_bucket_arn = aws_s3tables_table_bucket.wikistream.arn
}

resource "aws_s3tables_namespace" "silver" {
  namespace        = "silver"
  table_bucket_arn = aws_s3tables_table_bucket.wikistream.arn
}

resource "aws_s3tables_namespace" "gold" {
  namespace        = "gold"
  table_bucket_arn = aws_s3tables_table_bucket.wikistream.arn
}

resource "aws_s3tables_namespace" "dq_audit" {
  namespace        = "dq_audit"
  table_bucket_arn = aws_s3tables_table_bucket.wikistream.arn
}
