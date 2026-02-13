###############################################################################
# QA Environment - Variable Declarations
###############################################################################

variable "project_name" {
  description = "Name of the project."
  type        = string
  default     = "etl-orchestrator"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "qa"
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
  default     = "etl-qa-workgroup"
}

variable "redshift_database" {
  description = "Redshift database name."
  type        = string
  default     = "etl_qa"
}

variable "redshift_schema" {
  description = "Redshift schema."
  type        = string
  default     = "public"
}

variable "secret_arn" {
  description = "ARN of the Secrets Manager secret."
  type        = string
  default     = "arn:aws:secretsmanager:us-east-1:652615011843:secret:qa/redshift/credentials"
}

variable "sftp_secret_name" {
  description = "Name of the SFTP Secrets Manager secret."
  type        = string
  default     = "qa/sftp/log-transfer"
}

variable "glue_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 2
}
