output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}

output "bronze_path" {
  value = local.bronze_path
}

output "dimension_table" {
  value = "${aws_glue_catalog_database.cdc.name}.${aws_glue_catalog_table.dim_customer_scd2.name}"
}

output "glue_job_name" {
  value = aws_glue_job.scd2_daily.name
}

output "bronze_writer_role_arn" {
  value = aws_iam_role.bronze_writer.arn
}

output "msk_cluster_arn" {
  value = var.create_msk ? aws_msk_serverless_cluster.cdc[0].arn : null
}
