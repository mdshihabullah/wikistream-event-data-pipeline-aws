# =============================================================================
# Orchestration Module - Step Functions and EventBridge
# =============================================================================

locals {
  # Common Spark submit parameters for batch jobs
  base_spark_params = "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false"

  # Spark params with Iceberg catalog
  spark_catalog_params = "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.adaptive.enabled=true --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=${var.s3_tables_bucket_arn} --conf spark.sql.catalog.s3tablesbucket.client.region=${var.region}"

  # Full parameters for batch jobs (without Deequ)
  batch_spark_params = "${local.base_spark_params} --conf spark.jars.packages=${var.spark_packages} ${local.spark_catalog_params} --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.adaptive.skewJoin.enabled=true"

  # Full parameters for DQ jobs (with Deequ)
  dq_spark_params = "${local.base_spark_params} --conf spark.jars.packages=${var.spark_packages_with_deequ} ${local.spark_catalog_params} --py-files s3://${var.data_bucket_id}/spark/jobs/dq.zip"
}

# -----------------------------------------------------------------------------
# Unified Batch Pipeline State Machine
# -----------------------------------------------------------------------------

resource "aws_sfn_state_machine" "batch_pipeline" {
  name     = "${var.name_prefix}-batch-pipeline"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${path.module}/templates/batch_pipeline.json.tftpl", {
    emr_app_id         = var.emr_serverless_app_id
    emr_role_arn       = var.emr_serverless_role_arn
    data_bucket        = var.data_bucket_id
    s3_tables_arn      = var.s3_tables_bucket_arn
    sns_topic_arn      = var.sns_topic_arn
    name_prefix        = var.name_prefix
    region             = var.region
    account_id         = var.account_id
    job_timeout        = var.job_timeout_seconds
    wait_seconds       = var.batch_pipeline_wait_seconds
    batch_spark_params = local.batch_spark_params
    dq_spark_params    = local.dq_spark_params
  })

  tags = var.tags
}

# -----------------------------------------------------------------------------
# Silver Processing State Machine (Standalone)
# -----------------------------------------------------------------------------

resource "aws_sfn_state_machine" "silver_processing" {
  name     = "${var.name_prefix}-silver-processing"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${path.module}/templates/silver_processing.json.tftpl", {
    emr_app_id         = var.emr_serverless_app_id
    emr_role_arn       = var.emr_serverless_role_arn
    data_bucket        = var.data_bucket_id
    s3_tables_arn      = var.s3_tables_bucket_arn
    sns_topic_arn      = var.sns_topic_arn
    name_prefix        = var.name_prefix
    job_timeout        = var.job_timeout_seconds
    batch_spark_params = local.batch_spark_params
  })

  tags = var.tags
}

# -----------------------------------------------------------------------------
# Gold Processing State Machine (Standalone)
# -----------------------------------------------------------------------------

resource "aws_sfn_state_machine" "gold_processing" {
  name     = "${var.name_prefix}-gold-processing"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${path.module}/templates/gold_processing.json.tftpl", {
    emr_app_id         = var.emr_serverless_app_id
    emr_role_arn       = var.emr_serverless_role_arn
    data_bucket        = var.data_bucket_id
    s3_tables_arn      = var.s3_tables_bucket_arn
    sns_topic_arn      = var.sns_topic_arn
    name_prefix        = var.name_prefix
    job_timeout        = var.job_timeout_seconds
    batch_spark_params = local.batch_spark_params
  })

  tags = var.tags
}

# -----------------------------------------------------------------------------
# Data Quality State Machine (Standalone)
# -----------------------------------------------------------------------------

resource "aws_sfn_state_machine" "data_quality" {
  name     = "${var.name_prefix}-data-quality"
  role_arn = var.step_functions_role_arn

  definition = templatefile("${path.module}/templates/data_quality.json.tftpl", {
    emr_app_id      = var.emr_serverless_app_id
    emr_role_arn    = var.emr_serverless_role_arn
    data_bucket     = var.data_bucket_id
    s3_tables_arn   = var.s3_tables_bucket_arn
    sns_topic_arn   = var.sns_topic_arn
    name_prefix     = var.name_prefix
    job_timeout     = var.job_timeout_seconds
    dq_spark_params = local.dq_spark_params
  })

  tags = var.tags
}

# -----------------------------------------------------------------------------
# EventBridge Rules
# -----------------------------------------------------------------------------

# Primary schedule - Batch pipeline (DISABLED - pipeline is self-triggering)
resource "aws_cloudwatch_event_rule" "batch_pipeline_schedule" {
  name                = "${var.name_prefix}-batch-pipeline-schedule"
  description         = "Trigger unified batch pipeline - backup schedule"
  schedule_expression = "rate(15 minutes)"
  state               = "DISABLED"

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "batch_pipeline_schedule" {
  rule      = aws_cloudwatch_event_rule.batch_pipeline_schedule.name
  target_id = "batch-pipeline"
  arn       = aws_sfn_state_machine.batch_pipeline.arn
  role_arn  = var.eventbridge_sfn_role_arn
}

# Individual schedules (DISABLED - use batch-pipeline)
resource "aws_cloudwatch_event_rule" "silver_schedule" {
  name                = "${var.name_prefix}-silver-schedule"
  description         = "Individual Silver processing (use batch-pipeline for normal ops)"
  schedule_expression = "rate(5 minutes)"
  state               = "DISABLED"

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "silver_schedule" {
  rule      = aws_cloudwatch_event_rule.silver_schedule.name
  target_id = "silver-processing"
  arn       = aws_sfn_state_machine.silver_processing.arn
  role_arn  = var.eventbridge_sfn_role_arn
}

resource "aws_cloudwatch_event_rule" "gold_schedule" {
  name                = "${var.name_prefix}-gold-schedule"
  description         = "Individual Gold processing (use batch-pipeline for normal ops)"
  schedule_expression = "rate(5 minutes)"
  state               = "DISABLED"

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "gold_schedule" {
  rule      = aws_cloudwatch_event_rule.gold_schedule.name
  target_id = "gold-processing"
  arn       = aws_sfn_state_machine.gold_processing.arn
  role_arn  = var.eventbridge_sfn_role_arn
}

resource "aws_cloudwatch_event_rule" "data_quality_schedule" {
  name                = "${var.name_prefix}-data-quality-schedule"
  description         = "Individual Data Quality (use batch-pipeline for normal ops)"
  schedule_expression = "rate(15 minutes)"
  state               = "DISABLED"

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "data_quality_schedule" {
  rule      = aws_cloudwatch_event_rule.data_quality_schedule.name
  target_id = "data-quality"
  arn       = aws_sfn_state_machine.data_quality.arn
  role_arn  = var.eventbridge_sfn_role_arn
}

# -----------------------------------------------------------------------------
# EventBridge Policy for Step Functions
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "eventbridge_sfn" {
  name = "${var.name_prefix}-eventbridge-sfn-policy"
  role = split("/", var.eventbridge_sfn_role_arn)[1]

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["states:StartExecution"]
      Resource = [
        aws_sfn_state_machine.batch_pipeline.arn,
        aws_sfn_state_machine.silver_processing.arn,
        aws_sfn_state_machine.gold_processing.arn,
        aws_sfn_state_machine.data_quality.arn
      ]
    }]
  })
}
