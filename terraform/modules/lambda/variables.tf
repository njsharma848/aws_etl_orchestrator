variable "project_name" {
  description = "Name of the project, used as a prefix for resource naming"
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)"
  type        = string
}

variable "lambda_orchestrator_role_arn" {
  description = "IAM role ARN for the orchestrator Lambda function"
  type        = string
}

variable "lambda_sftp_role_arn" {
  description = "IAM role ARN for the SFTP log transfer Lambda function"
  type        = string
}

variable "sns_topic_arn" {
  description = "ARN of the SNS topic used by the orchestrator for notifications"
  type        = string
}

variable "step_function_arn" {
  description = "ARN of the Step Functions state machine invoked by the orchestrator"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket used by the SFTP log transfer Lambda"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket, used for the Lambda invocation permission"
  type        = string
}

variable "sqs_queue_arn" {
  description = "ARN of the SQS FIFO queue that triggers the orchestrator Lambda"
  type        = string
}

variable "sftp_secret_name" {
  description = "Secrets Manager secret name holding SFTP credentials"
  type        = string
  default     = "prod/sftp/log-transfer"
}

variable "orchestrator_source_dir" {
  description = "Path to the directory containing the orchestrator Lambda source code"
  type        = string
}

variable "sftp_source_dir" {
  description = "Path to the directory containing the SFTP log transfer Lambda source code"
  type        = string
}

variable "tags" {
  description = "Map of tags to apply to all resources"
  type        = map(string)
  default     = {}
}
