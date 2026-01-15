# =============================================================================
# Orchestration Module - Outputs
# =============================================================================

output "batch_pipeline_state_machine_arn" {
  description = "Unified batch pipeline state machine ARN"
  value       = aws_sfn_state_machine.batch_pipeline.arn
}

output "silver_state_machine_arn" {
  description = "Silver processing state machine ARN"
  value       = aws_sfn_state_machine.silver_processing.arn
}

output "gold_state_machine_arn" {
  description = "Gold processing state machine ARN"
  value       = aws_sfn_state_machine.gold_processing.arn
}

output "data_quality_state_machine_arn" {
  description = "Data quality state machine ARN"
  value       = aws_sfn_state_machine.data_quality.arn
}
