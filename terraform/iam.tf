data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${local.name}-glue-scd2"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

locals {
  glue_db_arn    = "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.cdc.name}"
  glue_table_arn = "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.cdc.name}/*"
  glue_cat_arn   = "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog"
}

# least privilege: read bronze + code, write only the dimension prefix, update only this database's tables
data "aws_iam_policy_document" "glue_job" {
  statement {
    sid       = "ListLake"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid       = "ReadBronzeAndCode"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/bronze/*", "${aws_s3_bucket.lake.arn}/artifacts/*"]
  }
  statement {
    sid       = "ReadWriteDimension"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/silver/dim_customer_scd2/*"]
  }
  statement {
    sid       = "UseLakeKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.lake.arn]
  }
  statement {
    sid       = "PromoteThroughCatalog"
    actions   = ["glue:GetTable", "glue:UpdateTable", "glue:GetDatabase"]
    resources = [local.glue_cat_arn, local.glue_db_arn, local.glue_table_arn]
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "scd2-lake-access"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

# Role for the bronze writer (ECS task / container reading the Debezium topic).
# It can only add objects under bronze/ - it never reads or changes the dimension.
data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bronze_writer" {
  name               = "${local.name}-bronze-writer"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "bronze_writer" {
  statement {
    sid       = "LandBronze"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake.arn}/bronze/*"]
  }
  statement {
    sid       = "EncryptWithLakeKey"
    actions   = ["kms:GenerateDataKey"]
    resources = [aws_kms_key.lake.arn]
  }

  dynamic "statement" {
    for_each = var.create_msk ? [1] : []
    content {
      sid     = "ConsumeCdcTopic"
      actions = ["kafka-cluster:Connect", "kafka-cluster:DescribeTopic", "kafka-cluster:ReadData", "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"]
      resources = [
        aws_msk_serverless_cluster.cdc[0].arn,
        "arn:aws:kafka:${var.region}:${data.aws_caller_identity.current.account_id}:topic/${aws_msk_serverless_cluster.cdc[0].cluster_name}/*/bank.public.customers",
        "arn:aws:kafka:${var.region}:${data.aws_caller_identity.current.account_id}:group/${aws_msk_serverless_cluster.cdc[0].cluster_name}/*/bronze-writer",
      ]
    }
  }
}

resource "aws_iam_role_policy" "bronze_writer" {
  name   = "bronze-landing"
  role   = aws_iam_role.bronze_writer.id
  policy = data.aws_iam_policy_document.bronze_writer.json
}
