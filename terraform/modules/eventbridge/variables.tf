################################################################################
# EventBridge Module - Variables
################################################################################

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket to monitor for object creation events"
  type        = string
}

variable "lambda_orchestrator_arn" {
  description = "ARN of the Lambda orchestrator function"
  type        = string
}

variable "lambda_orchestrator_function_name" {
  description = "Name of the Lambda orchestrator function"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
