###############################################################################
# Staging Environment - Backend & Provider Configuration
###############################################################################

terraform {
  backend "s3" {
    bucket         = "etl-orchestrator-tfstate-staging"
    key            = "etl-orchestrator/staging/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "etl-orchestrator-tflock-staging"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
}

module "etl_pipeline" {
  source = "../../"

  project_name            = var.project_name
  environment             = var.environment
  aws_region              = var.aws_region
  notification_email      = var.notification_email
  redshift_workgroup_name = var.redshift_workgroup_name
  redshift_database       = var.redshift_database
  redshift_schema         = var.redshift_schema
  sftp_secret_name        = var.sftp_secret_name
  glue_workers            = var.glue_workers
}
