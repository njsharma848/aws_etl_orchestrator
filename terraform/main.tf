###############################################################################
# Root Module - ETL Orchestrator Pipeline
#
# This root module wires together all sub-modules that compose the ETL
# pipeline: SNS notifications, S3 storage, IAM roles, Glue jobs,
# Step Functions state machine, Lambda functions, and EventBridge rules.
###############################################################################

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ------------------------------------------------------------------------------
# SNS - Notification topic for pipeline alerts
# ------------------------------------------------------------------------------
module "sns" {
  source             = "./modules/sns"
  project_name       = var.project_name
  environment        = var.environment
  notification_email = var.notification_email
  tags               = local.common_tags
}

# ------------------------------------------------------------------------------
# S3 - Data lake bucket for raw/processed/archive data
# ------------------------------------------------------------------------------
module "s3" {
  source       = "./modules/s3"
  project_name = var.project_name
  environment  = var.environment
  tags         = local.common_tags
}

# ------------------------------------------------------------------------------
# IAM - Roles and policies for Lambda, Glue, and Step Functions
# ------------------------------------------------------------------------------
module "iam" {
  source               = "./modules/iam"
  project_name         = var.project_name
  environment          = var.environment
  s3_bucket_arn        = module.s3.bucket_arn
  sns_topic_arn        = module.sns.topic_arn
  step_function_arn    = "*"  # Avoid circular dep: IAM <-> Step Functions
  redshift_workgroup_arn = var.redshift_workgroup_arn
  secret_arn           = var.secret_arn
  tags                 = local.common_tags
}

# ------------------------------------------------------------------------------
# Glue - ETL job for data transformation and Redshift loading
# ------------------------------------------------------------------------------
module "glue" {
  source                  = "./modules/glue"
  project_name            = var.project_name
  environment             = var.environment
  glue_role_arn           = module.iam.glue_role_arn
  s3_bucket_name          = module.s3.bucket_name
  redshift_workgroup_name = var.redshift_workgroup_name
  redshift_database       = var.redshift_database
  redshift_schema         = var.redshift_schema
  aws_region              = var.aws_region
  secret_arn              = var.secret_arn
  glue_workers            = var.glue_workers
  tags                    = local.common_tags
}

# ------------------------------------------------------------------------------
# Step Functions - State machine that orchestrates the Glue ETL job
# ------------------------------------------------------------------------------
module "step_functions" {
  source                  = "./modules/step_functions"
  project_name            = var.project_name
  environment             = var.environment
  step_functions_role_arn = module.iam.step_functions_role_arn
  glue_job_name           = module.glue.glue_job_name
  tags                    = local.common_tags
}

# ------------------------------------------------------------------------------
# Lambda - Orchestrator and SFTP log-transfer functions
# ------------------------------------------------------------------------------
module "lambda" {
  source                      = "./modules/lambda"
  project_name                = var.project_name
  environment                 = var.environment
  lambda_orchestrator_role_arn = module.iam.lambda_orchestrator_role_arn
  lambda_sftp_role_arn        = module.iam.lambda_sftp_role_arn
  sns_topic_arn               = module.sns.topic_arn
  step_function_arn           = module.step_functions.state_machine_arn
  s3_bucket_name              = module.s3.bucket_name
  s3_bucket_arn               = module.s3.bucket_arn
  sftp_secret_name            = var.sftp_secret_name
  orchestrator_source_dir     = "${path.module}/../LAMBDA_FUNCTIONS"
  sftp_source_dir             = "${path.module}/../LAMBDA_FUNCTIONS"
  tags                        = local.common_tags
}

# ------------------------------------------------------------------------------
# EventBridge - S3 event rule that triggers the orchestrator Lambda
# ------------------------------------------------------------------------------
module "eventbridge" {
  source                            = "./modules/eventbridge"
  project_name                      = var.project_name
  environment                       = var.environment
  s3_bucket_name                    = module.s3.bucket_name
  lambda_orchestrator_arn           = module.lambda.orchestrator_function_arn
  lambda_orchestrator_function_name = module.lambda.orchestrator_function_name
  tags                              = local.common_tags
}
