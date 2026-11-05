resource "aws_glue_catalog_database" "cdc" {
  name        = replace("${local.name}_cdc", "-", "_")
  description = "CDC lakehouse: customer SCD2 dimension built from Debezium change events"
}

# The table starts at a placeholder location. The Glue job points it at v=<run_id>/ with
# UpdateTable after DQ passes (pipeline/glue_catalog.py), so Terraform must not fight that
# change on the next apply: location and job-written parameters are ignored.
resource "aws_glue_catalog_table" "dim_customer_scd2" {
  database_name = aws_glue_catalog_database.cdc.name
  name          = "dim_customer_scd2"
  table_type    = "EXTERNAL_TABLE"
  description   = "SCD Type 2 customer dimension; one row per version, valid_from/valid_to/is_current"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  storage_descriptor {
    location      = "${local.table_root}/_empty/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = [
        ["customer_sk", "string"],
        ["customer_id", "bigint"],
        ["full_name", "string"],
        ["email", "string"],
        ["city", "string"],
        ["branch_code", "string"],
        ["risk_tier", "string"],
        ["kyc_status", "string"],
        ["updated_at", "string"],
        ["valid_from", "timestamp"],
        ["valid_to", "timestamp"],
        ["is_current", "boolean"],
        ["source_lsn", "bigint"],
        ["source_op", "string"],
        ["closed_by_lsn", "bigint"],
        ["ended_by_delete", "boolean"],
        ["row_hash", "string"],
      ]
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }

  lifecycle {
    ignore_changes = [storage_descriptor[0].location, parameters]
  }
}

# Bronze as an external table too, partitioned by event_date, for debugging and the
# reconcile queries. Partition projection means no crawler and no MSCK REPAIR.
resource "aws_glue_catalog_table" "bronze_customers_cdc" {
  database_name = aws_glue_catalog_database.cdc.name
  name          = "bronze_customers_cdc"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification                 = "json"
    "projection.enabled"           = "true"
    "projection.event_date.type"   = "date"
    "projection.event_date.format" = "yyyy-MM-dd"
    "projection.event_date.range"  = "2026-01-01,NOW"
    "storage.location.template"    = "${local.bronze_path}/event_date=$${event_date}/"
  }

  partition_keys {
    name = "event_date"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.bronze_path}/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters            = { "ignore.malformed.json" = "true" }
    }

    columns {
      name = "op"
      type = "string"
    }
    columns {
      name = "ts_ms"
      type = "bigint"
    }
    columns {
      name = "before"
      type = "struct<customer_id:bigint,full_name:string,email:string,city:string,branch_code:string,risk_tier:string,kyc_status:string,updated_at:string>"
    }
    columns {
      name = "after"
      type = "struct<customer_id:bigint,full_name:string,email:string,city:string,branch_code:string,risk_tier:string,kyc_status:string,updated_at:string>"
    }
    columns {
      name = "source"
      type = "struct<lsn:bigint,txid:bigint,ts_ms:bigint,snapshot:string>"
    }
    columns {
      name = "_lsn"
      type = "bigint"
    }
    columns {
      name = "_kafka_partition"
      type = "int"
    }
    columns {
      name = "_kafka_offset"
      type = "bigint"
    }
    columns {
      name = "_ingested_at"
      type = "string"
    }
  }
}

locals {
  glue_default_args = {
    "--job-language"                     = "python"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.lake.id}/${aws_s3_object.glue_libs.key}"
    "--bronze_path"                      = local.bronze_path
    "--table_root"                       = local.table_root
    "--database"                         = aws_glue_catalog_database.cdc.name
    "--table"                            = aws_glue_catalog_table.dim_customer_scd2.name
    "--lookback_days"                    = tostring(var.lookback_days)
    "--mode"                             = "auto"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "false"
    # bookmarks off: the job tracks its own state (live version on the table, LSN dedupe)
    "--job-bookmark-option" = "job-bookmark-disable"
    "--conf"                = "spark.sql.session.timeZone=UTC"
  }
}

resource "aws_glue_job" "scd2_daily" {
  name              = "${local.name}-scd2-daily"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_workers
  timeout           = 60 # minutes; a normal run is a few minutes
  max_retries       = 0  # a retry is a new run_id decision for the operator, not automatic

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.id}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1 # two runs promoting at once would race on UpdateTable
  }

  default_arguments = local.glue_default_args
}

resource "aws_glue_trigger" "daily_incremental" {
  name     = "${local.name}-scd2-daily"
  type     = "SCHEDULED"
  schedule = var.daily_schedule

  actions {
    job_name = aws_glue_job.scd2_daily.name
  }
}

resource "aws_glue_trigger" "weekly_full_rebuild" {
  name     = "${local.name}-scd2-weekly-full"
  type     = "SCHEDULED"
  schedule = var.weekly_full_schedule

  actions {
    job_name  = aws_glue_job.scd2_daily.name
    arguments = { "--mode" = "full" }
  }
}
