output "quicksight_data_source_arn" {
  description = "QuickSight Athena data source ARN"
  value       = aws_quicksight_data_source.athena.arn
}

output "quicksight_hourly_stats_dataset_arn" {
  description = "QuickSight dataset ARN for hourly_stats"
  value       = aws_quicksight_data_set.hourly_stats.arn
}

output "quicksight_risk_scores_dataset_arn" {
  description = "QuickSight dataset ARN for risk_scores"
  value       = aws_quicksight_data_set.risk_scores.arn
}

output "quicksight_daily_summary_dataset_arn" {
  description = "QuickSight dataset ARN for daily_analytics_summary"
  value       = aws_quicksight_data_set.daily_analytics_summary.arn
}
