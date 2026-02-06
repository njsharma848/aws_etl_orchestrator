###############################################################################
# Variables for IAM Module
###############################################################################

variable "project_name" {
  description = "Name of the project, used as a prefix for all IAM role names."
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket used by the ETL pipeline."
  type        = string
}

variable "sns_topic_arn" {
  description = "ARN of the SNS topic for pipeline notifications."
  type        = string
}

variable "step_function_arn" {
  description = "ARN of the Step Functions state machine. Use \"*\" before the state machine is created."
  type        = string
  default     = "*"
}

variable "redshift_workgroup_arn" {
  description = "ARN of the Redshift Serverless workgroup. Use \"*\" before the workgroup is created."
  type        = string
  default     = "*"
}

variable "secret_arn" {
  description = "ARN of the Secrets Manager secret (e.g. SFTP credentials, Redshift credentials)."
  type        = string
}

variable "tags" {
  description = "Map of tags to apply to all IAM resources."
  type        = map(string)
  default     = {}
}
