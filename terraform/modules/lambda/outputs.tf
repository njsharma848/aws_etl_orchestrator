output "orchestrator_function_arn" {
  description = "ARN of the orchestrator Lambda function"
  value       = aws_lambda_function.orchestrator.arn
}

output "orchestrator_function_name" {
  description = "Name of the orchestrator Lambda function"
  value       = aws_lambda_function.orchestrator.function_name
}

output "sftp_function_arn" {
  description = "ARN of the SFTP log transfer Lambda function"
  value       = aws_lambda_function.sftp_log_transfer.arn
}

output "sftp_function_name" {
  description = "Name of the SFTP log transfer Lambda function"
  value       = aws_lambda_function.sftp_log_transfer.function_name
}
