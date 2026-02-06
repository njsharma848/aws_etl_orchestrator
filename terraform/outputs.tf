###############################################################################
# Root Module Outputs - ETL Orchestrator Pipeline
###############################################################################

output "s3_bucket_name" {
  description = "Name of the S3 data-lake bucket."
  value       = module.s3.bucket_name
}

output "lambda_orchestrator_arn" {
  description = "ARN of the orchestrator Lambda function."
  value       = module.lambda.orchestrator_function_arn
}

output "glue_job_name" {
  description = "Name of the Glue ETL job."
  value       = module.glue.glue_job_name
}

output "state_machine_arn" {
  description = "ARN of the Step Functions state machine."
  value       = module.step_functions.state_machine_arn
}

output "sns_topic_arn" {
  description = "ARN of the SNS notification topic."
  value       = module.sns.topic_arn
}
