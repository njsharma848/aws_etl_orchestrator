###############################################################################
# Variables for Redshift Serverless Module
###############################################################################

variable "project_name" {
  description = "Name of the project, used as prefix for resource names."
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g. dev, qa, staging, prod)."
  type        = string
}

variable "workgroup_name" {
  description = "Name for the Redshift Serverless workgroup."
  type        = string
}

variable "database_name" {
  description = "Name of the Redshift database to create."
  type        = string
}

variable "admin_username" {
  description = "Admin username for the Redshift database."
  type        = string
  default     = "admin"
}

variable "base_capacity" {
  description = "Base RPU capacity for Redshift Serverless (minimum 8)."
  type        = number
  default     = 8
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket that Redshift reads from via COPY."
  type        = string
}

variable "tags" {
  description = "Map of tags to apply to all resources."
  type        = map(string)
  default     = {}
}
