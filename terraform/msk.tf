# Optional: MSK Serverless for the Debezium topic. Off by default because it bills per hour
# while idle; locally the same topic runs in docker/docker-compose.yml. Debezium itself
# would run on MSK Connect (custom plugin = Debezium Postgres connector zip) with the same
# settings as docker/connect/customers-connector.json.
resource "aws_msk_serverless_cluster" "cdc" {
  count        = var.create_msk ? 1 : 0
  cluster_name = "${local.name}-cdc"

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}
