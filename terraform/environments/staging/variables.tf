###############################################################################
# Staging Environment - Variable Declarations
###############################################################################

variable "project_name" {
  description = "Name of the project."
  type        = string
  default     = "etl-orchestrator"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "staging"
}

variable "aws_region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "notification_email" {
  description = "Email address for SNS notifications."
  type        = string
  default     = ""
}

variable "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name."
  type        = string
  default     = "etl-staging-workgroup"
}

variable "redshift_database" {
  description = "Redshift database name."
  type        = string
  default     = "etl_staging"
}

variable "redshift_schema" {
  description = "Redshift schema."
  type        = string
  default     = "public"
}

variable "sftp_secret_name" {
  description = "Name of the SFTP Secrets Manager secret."
  type        = string
  default     = "staging/sftp/log-transfer"
}

variable "glue_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 3
}
