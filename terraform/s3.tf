locals {
  name        = "${var.project}-${var.environment}"
  lake_bucket = "${local.name}-lake-${data.aws_caller_identity.current.account_id}"
  bronze_path = "s3://${local.lake_bucket}/bronze/customers_cdc"
  table_root  = "s3://${local.lake_bucket}/silver/dim_customer_scd2"
}

# one bucket, prefixes per layer: bronze/ (raw CDC), silver/ (dimension versions), artifacts/ (Glue code)
resource "aws_s3_bucket" "lake" {
  bucket = local.lake_bucket
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# customer PII: encrypt with a KMS key we control, bucket key keeps KMS request cost down
resource "aws_kms_key" "lake" {
  description             = "${local.name} lake encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "lake" {
  name          = "alias/${local.name}-lake"
  target_key_id = aws_kms_key.lake.key_id
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true
  }
}

# bronze is the replayable source of truth for the full rebuild, so versioning protects it from a bad delete
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-rejected-cdc-events"
    status = "Enabled"
    filter {
      prefix = "bronze/_rejected/"
    }
    expiration {
      days = var.bronze_rejected_expiry_days
    }
  }

  rule {
    id     = "bronze-to-infrequent-access"
    status = "Enabled"
    filter {
      prefix = "bronze/customers_cdc/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "clean-noncurrent-object-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Glue job code: thin script + zipped packages built by scripts/package_glue.sh
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "artifacts/glue/scd2_daily_job.py"
  source = "${path.module}/../glue/scd2_daily_job.py"
  etag   = filemd5("${path.module}/../glue/scd2_daily_job.py")
}

resource "aws_s3_object" "glue_libs" {
  bucket      = aws_s3_bucket.lake.id
  key         = "artifacts/glue/cdc_pipeline.zip"
  source      = "${path.module}/../dist/cdc_pipeline.zip"
  source_hash = filemd5("${path.module}/../dist/cdc_pipeline.zip")
}
