###############################################################################
# Root Module Variables - ETL Orchestrator Pipeline
###############################################################################

variable "project_name" {
  description = "Name of the project. Used as a prefix for all resource names."
  type        = string
  default     = "etl-orchestrator"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, qa, staging, prod)."
  type        = string
}

variable "aws_region" {
  description = "AWS region where all resources will be created."
  type        = string
  default     = "us-east-1"
}

variable "notification_email" {
  description = "Email address subscribed to the SNS notification topic."
  type        = string
  default     = ""
}

variable "redshift_workgroup_name" {
  description = "Name of the Redshift Serverless workgroup used by the Glue job."
  type        = string
}

variable "redshift_database" {
  description = "Name of the Redshift database that the Glue job writes to."
  type        = string
}

variable "redshift_schema" {
  description = "Redshift schema used by the Glue job."
  type        = string
  default     = "public"
}

variable "redshift_base_capacity" {
  description = "Base RPU capacity for Redshift Serverless (minimum 8)."
  type        = number
  default     = 8
}

variable "sftp_secret_name" {
  description = "Name (not ARN) of the Secrets Manager secret for SFTP credentials."
  type        = string
  default     = "prod/sftp/log-transfer"
}

variable "glue_workers" {
  description = "Number of Glue workers allocated to the ETL job."
  type        = number
  default     = 2
}
