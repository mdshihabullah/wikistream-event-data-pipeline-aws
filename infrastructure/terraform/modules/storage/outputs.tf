# =============================================================================
# Storage Module - Outputs
# =============================================================================

output "data_bucket_id" {
  description = "S3 data bucket ID"
  value       = aws_s3_bucket.data.id
}

output "data_bucket_arn" {
  description = "S3 data bucket ARN"
  value       = aws_s3_bucket.data.arn
}

output "s3_tables_bucket_arn" {
  description = "S3 Tables bucket ARN"
  value       = aws_s3tables_table_bucket.wikistream.arn
}

output "s3_tables_bucket_name" {
  description = "S3 Tables bucket name"
  value       = aws_s3tables_table_bucket.wikistream.name
}

output "namespace_bronze" {
  description = "Bronze namespace name"
  value       = aws_s3tables_namespace.bronze.namespace
}

output "namespace_silver" {
  description = "Silver namespace name"
  value       = aws_s3tables_namespace.silver.namespace
}

output "namespace_gold" {
  description = "Gold namespace name"
  value       = aws_s3tables_namespace.gold.namespace
}

output "namespace_dq_audit" {
  description = "DQ Audit namespace name"
  value       = aws_s3tables_namespace.dq_audit.namespace
}
