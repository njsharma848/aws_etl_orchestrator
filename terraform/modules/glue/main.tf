################################################################################
# AWS Glue Job
################################################################################

resource "aws_glue_job" "etl_job" {
  name     = "${var.project_name}-${var.environment}-etl-job"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.s3_bucket_name}/scripts/glue_job.py"
  }

  default_arguments = {
    "--job-language"                          = "python"
    "--TempDir"                               = "s3://${var.s3_bucket_name}/temp/"
    "--enable-metrics"                        = "true"
    "--enable-continuous-cloudwatch-log"       = "true"
    "--workgroup_name"                        = var.redshift_workgroup_name
    "--database"                              = var.redshift_database
    "--region"                                = var.aws_region
    "--secret_arn"                            = var.secret_arn
    "--iam_role"                              = var.glue_role_arn
    "--schema_name"                           = var.redshift_schema
    "--src_bucket"                            = var.s3_bucket_name
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  max_retries       = 1
  timeout           = 120

  tags = var.tags
}
