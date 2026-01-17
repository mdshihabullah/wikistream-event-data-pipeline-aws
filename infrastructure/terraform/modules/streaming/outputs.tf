# =============================================================================
# Streaming Module - Outputs
# =============================================================================

output "cluster_arn" {
  description = "MSK cluster ARN"
  value       = aws_msk_cluster.wikistream.arn
}

output "cluster_name" {
  description = "MSK cluster name"
  value       = aws_msk_cluster.wikistream.cluster_name
}

output "bootstrap_brokers_iam" {
  description = "MSK bootstrap brokers for IAM authentication"
  value       = aws_msk_cluster.wikistream.bootstrap_brokers_sasl_iam
  sensitive   = true
}

output "log_group_name" {
  description = "CloudWatch log group name for MSK"
  value       = aws_cloudwatch_log_group.msk.name
}
