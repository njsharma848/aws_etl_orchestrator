locals {
  bucket_name = "${var.project_name}-${var.environment}-data"

  default_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  tags = merge(local.default_tags, var.tags)
}

# ------------------------------------------------------------------------------
# S3 Bucket
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "etl_data" {
  bucket        = local.bucket_name
  force_destroy = true
  tags          = local.tags
}

# ------------------------------------------------------------------------------
# Versioning
# ------------------------------------------------------------------------------
resource "aws_s3_bucket_versioning" "etl_data" {
  bucket = aws_s3_bucket.etl_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ------------------------------------------------------------------------------
# Server-Side Encryption (SSE-S3 / AES256)
# ------------------------------------------------------------------------------
resource "aws_s3_bucket_server_side_encryption_configuration" "etl_data" {
  bucket = aws_s3_bucket.etl_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ------------------------------------------------------------------------------
# Lifecycle Rules
#   - Transition data/archive/ objects to GLACIER after 90 days
#   - Expire data/staging/ objects after 7 days
# ------------------------------------------------------------------------------
resource "aws_s3_bucket_lifecycle_configuration" "etl_data" {
  bucket = aws_s3_bucket.etl_data.id

  rule {
    id     = "archive-to-glacier"
    status = "Enabled"

    filter {
      prefix = "data/archive/"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "expire-staging"
    status = "Enabled"

    filter {
      prefix = "data/staging/"
    }

    expiration {
      days = 7
    }
  }
}

# ------------------------------------------------------------------------------
# Block All Public Access
# ------------------------------------------------------------------------------
resource "aws_s3_bucket_public_access_block" "etl_data" {
  bucket = aws_s3_bucket.etl_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------------------------
# EventBridge Notifications
#   Enables EventBridge to receive events from this bucket. The actual
#   EventBridge rule is managed by the EventBridge module, so an
#   aws_s3_bucket_notification resource is not required here.
# ------------------------------------------------------------------------------
resource "aws_s3_bucket_notification" "etl_data" {
  bucket      = aws_s3_bucket.etl_data.id
  eventbridge = true
}

# ------------------------------------------------------------------------------
# Pre-create S3 Folder Structure
#   S3 "folders" are zero-byte objects with a trailing slash. Pre-creating
#   them makes the bucket layout visible in the console and documents the
#   expected directory contract for the pipeline.
# ------------------------------------------------------------------------------
locals {
  s3_folders = [
    "data/in/",
    "data/staging/",
    "data/archive/",
    "data/unprocessed/",
    "data/new_files/",
    "config/",
    "scripts/",
    "logs/",
  ]
}

resource "aws_s3_object" "folders" {
  for_each = toset(local.s3_folders)

  bucket  = aws_s3_bucket.etl_data.id
  key     = each.value
  content = ""
}
