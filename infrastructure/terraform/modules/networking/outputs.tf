# =============================================================================
# Networking Module - Outputs
# =============================================================================

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "vpc_cidr" {
  description = "VPC CIDR block"
  value       = module.vpc.vpc_cidr_block
}

output "private_subnets" {
  description = "List of private subnet IDs"
  value       = module.vpc.private_subnets
}

output "public_subnets" {
  description = "List of public subnet IDs"
  value       = module.vpc.public_subnets
}

output "msk_security_group_id" {
  description = "Security group ID for MSK"
  value       = aws_security_group.msk.id
}

output "ecs_security_group_id" {
  description = "Security group ID for ECS"
  value       = aws_security_group.ecs.id
}

output "emr_security_group_id" {
  description = "Security group ID for EMR Serverless"
  value       = aws_security_group.emr.id
}
