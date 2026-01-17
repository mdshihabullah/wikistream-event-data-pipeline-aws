# =============================================================================
# Streaming Module - MSK Kafka Cluster
# =============================================================================
# Creates MSK cluster with KRaft mode (Kafka 3.9.x without Zookeeper)
# =============================================================================

# -----------------------------------------------------------------------------
# CloudWatch Log Group for MSK
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.name_prefix}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# -----------------------------------------------------------------------------
# MSK Configuration
# -----------------------------------------------------------------------------

resource "aws_msk_configuration" "wikistream" {
  name           = "${var.name_prefix}-msk-config"
  kafka_versions = ["3.9.x"]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=true
    default.replication.factor=2
    min.insync.replicas=1
    num.partitions=6
    log.retention.hours=${var.retention_hours}
    log.retention.bytes=-1
    compression.type=snappy
  PROPERTIES
}

# -----------------------------------------------------------------------------
# MSK Cluster (KRaft Mode)
# -----------------------------------------------------------------------------

resource "aws_msk_cluster" "wikistream" {
  cluster_name           = "${var.name_prefix}-msk"
  kafka_version          = "3.9.x"
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.instance_type
    client_subnets  = var.private_subnets
    security_groups = [var.security_group_id]

    storage_info {
      ebs_storage_info {
        volume_size = var.storage_size
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.wikistream.arn
    revision = aws_msk_configuration.wikistream.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = var.tags
}
