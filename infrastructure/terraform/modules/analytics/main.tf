locals {
  quicksight_user_arn = "arn:aws:quicksight:${var.region}:${var.account_id}:user/${var.quicksight_namespace}/${var.quicksight_user_name}"
  s3tables_catalog_id = "${var.account_id}:s3tablescatalog/${var.s3_tables_bucket_name}"
  s3tables_catalog    = "s3tablescatalog"
  quicksight_tables   = ["hourly_stats", "risk_scores", "daily_analytics_summary"]
}

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "lakeformation_admin" {
  name = "${var.name_prefix}-lakeformation-admin"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          AWS = "arn:aws:iam::${var.account_id}:root"
        }
      },
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = "lakeformation.amazonaws.com"
        }
      }
    ]
  })
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "lakeformation_admin" {
  role       = aws_iam_role.lakeformation_admin.name
  policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"
}

resource "aws_iam_role_policy" "lakeformation_data_access" {
  name = "${var.name_prefix}-lakeformation-s3tables-access"
  role = aws_iam_role.lakeformation_admin.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3TablesAccess"
        Effect   = "Allow"
        Action   = ["s3tables:*"]
        Resource = ["arn:aws:s3tables:${var.region}:${var.account_id}:bucket/*"]
      }
    ]
  })
}

locals {
  lakeformation_admin_arns = distinct([
    data.aws_caller_identity.current.arn,
    aws_iam_role.lakeformation_admin.arn
  ])
}

resource "null_resource" "lakeformation_admin_settings" {
  triggers = {
    admins = join(",", local.lakeformation_admin_arns)
  }

  provisioner "local-exec" {
    command = <<EOT
sleep 10
aws lakeformation put-data-lake-settings \
  --region ${var.region} \
  --cli-input-json '${jsonencode({
  DataLakeSettings = {
    DataLakeAdmins = [
      for admin_arn in local.lakeformation_admin_arns : {
        DataLakePrincipalIdentifier = admin_arn
      }
    ]
  }
})}'
EOT
  }

  depends_on = [
    aws_iam_role.lakeformation_admin,
    aws_iam_role_policy_attachment.lakeformation_admin,
    aws_iam_role_policy.lakeformation_data_access
  ]
}

resource "null_resource" "s3tables_catalog_integration" {
  triggers = {
    bucket_arn = "arn:aws:s3tables:${var.region}:${var.account_id}:bucket/*"
    role_arn   = aws_iam_role.lakeformation_admin.arn
    region     = var.region
  }

  provisioner "local-exec" {
    command = <<EOT
aws lakeformation register-resource \
  --region ${self.triggers.region} \
  --resource-arn '${self.triggers.bucket_arn}' \
  --role-arn '${self.triggers.role_arn}' \
  --with-federation \
  --with-privileged-access || true
EOT
  }

  provisioner "local-exec" {
    command = <<EOT
aws glue create-catalog \
  --region ${self.triggers.region} \
  --cli-input-json '${jsonencode({
  Name = "s3tablescatalog",
  CatalogInput = {
    FederatedCatalog = {
      Identifier = "arn:aws:s3tables:${var.region}:${var.account_id}:bucket/*",
      ConnectionName = "aws:s3tables"
    },
    CreateDatabaseDefaultPermissions = [],
    CreateTableDefaultPermissions = []
  }
})}' || true
EOT
  }
}

resource "null_resource" "athena_s3tables_catalog" {
  triggers = {
    catalog_id        = local.s3tables_catalog_id
    region            = var.region
    reconcile_version = "1"
  }

  provisioner "local-exec" {
    command = <<EOT
aws athena update-data-catalog \
  --region ${self.triggers.region} \
  --name s3tablescatalog \
  --type GLUE \
  --description "S3 Tables Glue catalog" \
  --parameters catalog-id=${self.triggers.catalog_id} || \
aws athena create-data-catalog \
  --region ${self.triggers.region} \
  --name s3tablescatalog \
  --type GLUE \
  --description "S3 Tables Glue catalog" \
  --parameters catalog-id=${self.triggers.catalog_id} || true
EOT
  }

  depends_on = [null_resource.s3tables_catalog_integration]
}

resource "aws_quicksight_account_subscription" "this" {
  count                 = var.quicksight_enable_subscription ? 1 : 0
  account_name          = var.quicksight_account_name
  authentication_method = var.quicksight_authentication_method
  aws_account_id        = var.account_id
  edition               = var.quicksight_edition
  notification_email    = var.quicksight_admin_email
}

data "aws_iam_role" "quicksight_service_role" {
  name       = var.quicksight_service_role_name
  depends_on = [aws_quicksight_account_subscription.this]
}

resource "aws_iam_role_policy" "quicksight_glue_catalog" {
  name = "${var.name_prefix}-quicksight-glue-catalog"
  role = data.aws_iam_role.quicksight_service_role.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "QuickSightGlueCatalogAccess"
        Effect   = "Allow"
        Action   = ["glue:GetCatalog", "glue:GetDatabases", "glue:GetDatabase", "glue:GetTables", "glue:GetTable"]
        Resource = "*"
      },
      {
        Sid      = "QuickSightAthenaAccess"
        Effect   = "Allow"
        Action   = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup",
          "athena:ListWorkGroups",
          "athena:GetDataCatalog",
          "athena:ListDataCatalogs",
          "athena:ListDatabases",
          "athena:GetDatabase",
          "athena:ListTableMetadata",
          "athena:GetTableMetadata"
        ]
        Resource = "*"
      },
      {
        Sid      = "QuickSightLakeFormationAccess"
        Effect   = "Allow"
        Action   = ["lakeformation:GetDataAccess"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_quicksight_data_source" "athena" {
  aws_account_id = var.account_id
  data_source_id = "${var.name_prefix}-athena"
  name           = "${var.name_prefix}-athena"
  type           = "ATHENA"

  parameters {
    athena {
      work_group = var.quicksight_athena_workgroup
    }
  }

  permission {
    principal = local.quicksight_user_arn
    actions = [
      "quicksight:DescribeDataSource",
      "quicksight:DescribeDataSourcePermissions",
      "quicksight:PassDataSource",
      "quicksight:UpdateDataSource",
      "quicksight:UpdateDataSourcePermissions",
      "quicksight:DeleteDataSource"
    ]
  }

  depends_on = [aws_iam_role_policy.quicksight_glue_catalog]
}

resource "aws_quicksight_data_set" "hourly_stats" {
  aws_account_id = var.account_id
  data_set_id    = "${var.name_prefix}-hourly-stats"
  name           = "${var.name_prefix}-hourly-stats"
  import_mode    = var.quicksight_import_mode

  physical_table_map {
    physical_table_map_id = "hourly-stats"
    custom_sql {
      data_source_arn = aws_quicksight_data_source.athena.arn
      name            = "hourly_stats"
      sql_query       = "SELECT * FROM \"${local.s3tables_catalog}\".gold.hourly_stats"

      columns {
        name = "stat_date"
        type = "STRING"
      }
      columns {
        name = "stat_hour"
        type = "INTEGER"
      }
      columns {
        name = "domain"
        type = "STRING"
      }
      columns {
        name = "region"
        type = "STRING"
      }
      columns {
        name = "total_events"
        type = "DECIMAL"
      }
      columns {
        name = "unique_users"
        type = "DECIMAL"
      }
      columns {
        name = "unique_pages"
        type = "DECIMAL"
      }
      columns {
        name = "bytes_added"
        type = "DECIMAL"
      }
      columns {
        name = "bytes_removed"
        type = "DECIMAL"
      }
      columns {
        name = "avg_edit_size"
        type = "DECIMAL"
      }
      columns {
        name = "bot_edits"
        type = "DECIMAL"
      }
      columns {
        name = "human_edits"
        type = "DECIMAL"
      }
      columns {
        name = "bot_percentage"
        type = "DECIMAL"
      }
      columns {
        name = "anonymous_edits"
        type = "DECIMAL"
      }
      columns {
        name = "type_edit"
        type = "DECIMAL"
      }
      columns {
        name = "type_new"
        type = "DECIMAL"
      }
      columns {
        name = "type_categorize"
        type = "DECIMAL"
      }
      columns {
        name = "type_log"
        type = "DECIMAL"
      }
      columns {
        name = "large_deletions"
        type = "DECIMAL"
      }
      columns {
        name = "large_additions"
        type = "DECIMAL"
      }
      columns {
        name = "gold_processed_at"
        type = "DATETIME"
      }
      columns {
        name = "schema_version"
        type = "STRING"
      }
    }
  }

  permissions {
    principal = local.quicksight_user_arn
    actions = [
      "quicksight:DescribeDataSet",
      "quicksight:DescribeDataSetPermissions",
      "quicksight:PassDataSet",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
      "quicksight:UpdateDataSet",
      "quicksight:UpdateDataSetPermissions",
      "quicksight:DeleteDataSet",
      "quicksight:CreateIngestion",
      "quicksight:CancelIngestion"
    ]
  }
}

resource "aws_quicksight_data_set" "risk_scores" {
  aws_account_id = var.account_id
  data_set_id    = "${var.name_prefix}-risk-scores"
  name           = "${var.name_prefix}-risk-scores"
  import_mode    = var.quicksight_import_mode

  physical_table_map {
    physical_table_map_id = "risk-scores"
    custom_sql {
      data_source_arn = aws_quicksight_data_source.athena.arn
      name            = "risk_scores"
      sql_query       = "SELECT * FROM \"${local.s3tables_catalog}\".gold.risk_scores"

      columns {
        name = "stat_date"
        type = "STRING"
      }
      columns {
        name = "entity_id"
        type = "STRING"
      }
      columns {
        name = "entity_type"
        type = "STRING"
      }
      columns {
        name = "total_edits"
        type = "DECIMAL"
      }
      columns {
        name = "edits_per_hour_avg"
        type = "DECIMAL"
      }
      columns {
        name = "large_deletions"
        type = "DECIMAL"
      }
      columns {
        name = "domains_edited"
        type = "DECIMAL"
      }
      columns {
        name = "risk_score"
        type = "DECIMAL"
      }
      columns {
        name = "risk_level"
        type = "STRING"
      }
      columns {
        name = "evidence"
        type = "STRING"
      }
      columns {
        name = "alert_triggered"
        type = "BOOLEAN"
      }
      columns {
        name = "gold_processed_at"
        type = "DATETIME"
      }
      columns {
        name = "schema_version"
        type = "STRING"
      }
    }
  }

  permissions {
    principal = local.quicksight_user_arn
    actions = [
      "quicksight:DescribeDataSet",
      "quicksight:DescribeDataSetPermissions",
      "quicksight:PassDataSet",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
      "quicksight:UpdateDataSet",
      "quicksight:UpdateDataSetPermissions",
      "quicksight:DeleteDataSet",
      "quicksight:CreateIngestion",
      "quicksight:CancelIngestion"
    ]
  }
}

resource "aws_quicksight_data_set" "daily_analytics_summary" {
  aws_account_id = var.account_id
  data_set_id    = "${var.name_prefix}-daily-analytics-summary"
  name           = "${var.name_prefix}-daily-analytics-summary"
  import_mode    = var.quicksight_import_mode

  physical_table_map {
    physical_table_map_id = "daily-analytics-summary"
    custom_sql {
      data_source_arn = aws_quicksight_data_source.athena.arn
      name            = "daily_analytics_summary"
      sql_query       = "SELECT * FROM \"${local.s3tables_catalog}\".gold.daily_analytics_summary"

      columns {
        name = "summary_date"
        type = "STRING"
      }
      columns {
        name = "total_events"
        type = "DECIMAL"
      }
      columns {
        name = "unique_users"
        type = "DECIMAL"
      }
      columns {
        name = "active_domains"
        type = "DECIMAL"
      }
      columns {
        name = "unique_pages_edited"
        type = "DECIMAL"
      }
      columns {
        name = "bot_percentage"
        type = "DECIMAL"
      }
      columns {
        name = "anonymous_percentage"
        type = "DECIMAL"
      }
      columns {
        name = "registered_user_percentage"
        type = "DECIMAL"
      }
      columns {
        name = "total_bytes_added"
        type = "DECIMAL"
      }
      columns {
        name = "total_bytes_removed"
        type = "DECIMAL"
      }
      columns {
        name = "net_content_change"
        type = "DECIMAL"
      }
      columns {
        name = "avg_edit_size_bytes"
        type = "DECIMAL"
      }
      columns {
        name = "new_pages_created"
        type = "DECIMAL"
      }
      columns {
        name = "large_deletions_count"
        type = "DECIMAL"
      }
      columns {
        name = "large_additions_count"
        type = "DECIMAL"
      }
      columns {
        name = "large_deletion_rate"
        type = "DECIMAL"
      }
      columns {
        name = "high_risk_user_count"
        type = "DECIMAL"
      }
      columns {
        name = "medium_risk_user_count"
        type = "DECIMAL"
      }
      columns {
        name = "low_risk_user_count"
        type = "DECIMAL"
      }
      columns {
        name = "platform_avg_risk_score"
        type = "DECIMAL"
      }
      columns {
        name = "platform_max_risk_score"
        type = "DECIMAL"
      }
      columns {
        name = "total_alerts_triggered"
        type = "DECIMAL"
      }
      columns {
        name = "europe_percentage"
        type = "DECIMAL"
      }
      columns {
        name = "americas_percentage"
        type = "DECIMAL"
      }
      columns {
        name = "asia_pacific_percentage"
        type = "DECIMAL"
      }
      columns {
        name = "peak_hour_events"
        type = "DECIMAL"
      }
      columns {
        name = "avg_events_per_hour"
        type = "DECIMAL"
      }
      columns {
        name = "platform_health_score"
        type = "DECIMAL"
      }
      columns {
        name = "gold_processed_at"
        type = "DATETIME"
      }
      columns {
        name = "schema_version"
        type = "STRING"
      }
    }
  }

  permissions {
    principal = local.quicksight_user_arn
    actions = [
      "quicksight:DescribeDataSet",
      "quicksight:DescribeDataSetPermissions",
      "quicksight:PassDataSet",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
      "quicksight:UpdateDataSet",
      "quicksight:UpdateDataSetPermissions",
      "quicksight:DeleteDataSet",
      "quicksight:CreateIngestion",
      "quicksight:CancelIngestion"
    ]
  }
}

locals {
  lakeformation_principals = {
    quicksight_user = local.quicksight_user_arn
    service_role    = data.aws_iam_role.quicksight_service_role.arn
    caller          = data.aws_caller_identity.current.arn
  }
  lakeformation_table_grants = {
    for item in flatten([
      for principal_key, principal_arn in local.lakeformation_principals : [
        for table_name in local.quicksight_tables : {
          key       = "${principal_key}-${table_name}"
          principal = principal_arn
          table     = table_name
        }
      ]
    ]) : item.key => {
      principal = item.principal
      table     = item.table
    }
  }
}

resource "null_resource" "lakeformation_database_grants" {
  for_each = var.quicksight_enable_lakeformation_permissions ? local.lakeformation_principals : {}

  triggers = {
    principal  = each.value
    catalog_id = local.s3tables_catalog_id
    region     = var.region
    reconcile_version = "1"
  }

  provisioner "local-exec" {
    command = <<EOT
aws lakeformation grant-permissions \
  --region ${var.region} \
  --cli-input-json '${jsonencode({
  Principal = { DataLakePrincipalIdentifier = each.value }
  Resource  = { Database = { Name = "gold", CatalogId = local.s3tables_catalog_id } }
  Permissions = ["DESCRIBE"]
})}' || true
EOT
  }

  provisioner "local-exec" {
    when = destroy
    command = <<EOT
aws lakeformation revoke-permissions \
  --region ${self.triggers.region} \
  --cli-input-json '${jsonencode({
  Principal = { DataLakePrincipalIdentifier = self.triggers.principal }
  Resource  = { Database = { Name = "gold", CatalogId = self.triggers.catalog_id } }
  Permissions = ["DESCRIBE"]
})}' || true
EOT
  }
}

resource "null_resource" "lakeformation_table_grants" {
  for_each = var.quicksight_enable_lakeformation_permissions ? local.lakeformation_table_grants : {}

  triggers = {
    principal  = each.value.principal
    table      = each.value.table
    catalog_id = local.s3tables_catalog_id
    region     = var.region
    reconcile_version = "1"
  }

  provisioner "local-exec" {
    command = <<EOT
aws lakeformation grant-permissions \
  --region ${var.region} \
  --cli-input-json '${jsonencode({
  Principal = { DataLakePrincipalIdentifier = each.value.principal }
  Resource  = { Table = { CatalogId = local.s3tables_catalog_id, DatabaseName = "gold", Name = each.value.table } }
  Permissions = ["SELECT", "DESCRIBE"]
})}' || true
EOT
  }

  provisioner "local-exec" {
    when = destroy
    command = <<EOT
aws lakeformation revoke-permissions \
  --region ${self.triggers.region} \
  --cli-input-json '${jsonencode({
  Principal = { DataLakePrincipalIdentifier = self.triggers.principal }
  Resource  = { Table = { CatalogId = self.triggers.catalog_id, DatabaseName = "gold", Name = self.triggers.table } }
  Permissions = ["SELECT", "DESCRIBE"]
})}' || true
EOT
  }
}
