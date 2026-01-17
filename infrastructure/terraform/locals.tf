# =============================================================================
# WikiStream Pipeline - Local Values
# =============================================================================
# Centralized local values for consistency across all modules
# =============================================================================

locals {
  # AWS Account and Region
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  # Resource naming prefix
  name_prefix = "${var.project_name}-${var.environment}"

  # Availability zones (2 AZs for cost savings in dev, can be 3 in prod)
  azs = slice(data.aws_availability_zones.available.names, 0, var.az_count)

  # Common tags applied to all resources
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }

  # Environment-specific settings
  is_production = var.environment == "prod"

  # Iceberg/Spark package versions (centralized for consistency)
  iceberg_version     = "1.10.0"
  s3tables_version    = "0.1.8"
  aws_sdk_version     = "2.29.0"
  deequ_version       = "2.0.7-spark-3.5"
  spark_kafka_version = "3.5.0"
  msk_iam_version     = "2.2.0"

  # Common Spark configurations
  spark_packages = join(",", [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${local.iceberg_version}",
    "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:${local.s3tables_version}",
    "software.amazon.awssdk:bundle:${local.aws_sdk_version}"
  ])

  spark_packages_with_kafka = join(",", [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${local.iceberg_version}",
    "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:${local.s3tables_version}",
    "software.amazon.awssdk:bundle:${local.aws_sdk_version}",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:${local.spark_kafka_version}",
    "software.amazon.msk:aws-msk-iam-auth:${local.msk_iam_version}"
  ])

  spark_packages_with_deequ = join(",", [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${local.iceberg_version}",
    "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:${local.s3tables_version}",
    "software.amazon.awssdk:bundle:${local.aws_sdk_version}",
    "com.amazon.deequ:deequ:${local.deequ_version}"
  ])
}
