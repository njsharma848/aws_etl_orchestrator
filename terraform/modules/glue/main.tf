################################################################################
# AWS Glue Jobs
#   - Simple ETL: CSV -> Redshift upsert (no dimensional model)
#   - Data Model ETL: CSV -> Redshift + star schema (dimensions + facts)
################################################################################

locals {
  common_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${var.s3_bucket_name}/temp/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--workgroup_name"                   = var.redshift_workgroup_name
    "--database"                         = var.redshift_database
    "--region"                           = var.aws_region
    "--secret_arn"                       = var.secret_arn
    "--iam_role"                         = var.redshift_copy_role_arn
    "--schema_name"                      = var.redshift_schema
    "--src_bucket"                       = var.s3_bucket_name
  }
}

# ------------------------------------------------------------------------------
# Simple ETL Job (without data model)
# ------------------------------------------------------------------------------
resource "aws_glue_job" "etl_simple" {
  name     = "${var.project_name}-${var.environment}-etl-simple"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.s3_bucket_name}/scripts/glue_job.py"
  }

  default_arguments = local.common_arguments

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  max_retries       = 1
  timeout           = 120

  tags = var.tags
}

# ------------------------------------------------------------------------------
# Data Model ETL Job (with dimensional model - star schema)
# ------------------------------------------------------------------------------
resource "aws_glue_job" "etl_data_model" {
  name     = "${var.project_name}-${var.environment}-etl-data-model"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.s3_bucket_name}/scripts/glue_job_with_data_model.py"
  }

  default_arguments = local.common_arguments

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  max_retries       = 1
  timeout           = 120

  tags = var.tags
}
