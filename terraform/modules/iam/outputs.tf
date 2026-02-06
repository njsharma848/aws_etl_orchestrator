###############################################################################
# Outputs for IAM Module
###############################################################################

output "lambda_orchestrator_role_arn" {
  description = "ARN of the Lambda Orchestrator IAM role."
  value       = aws_iam_role.lambda_orchestrator.arn
}

output "lambda_sftp_role_arn" {
  description = "ARN of the Lambda SFTP IAM role."
  value       = aws_iam_role.lambda_sftp.arn
}

output "glue_role_arn" {
  description = "ARN of the Glue ETL IAM role."
  value       = aws_iam_role.glue_etl.arn
}

output "step_functions_role_arn" {
  description = "ARN of the Step Functions Orchestrator IAM role."
  value       = aws_iam_role.sfn_orchestrator.arn
}
