################################################################################
# Glue Module - Variables
################################################################################

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for the Glue job"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for scripts and temp storage"
  type        = string
}

variable "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name"
  type        = string
}

variable "redshift_database" {
  description = "Redshift database name"
  type        = string
}

variable "redshift_schema" {
  description = "Redshift schema name"
  type        = string
  default     = "public"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "secret_arn" {
  description = "ARN of the Secrets Manager secret"
  type        = string
}

variable "glue_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 2
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
