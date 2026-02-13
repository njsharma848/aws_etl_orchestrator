###############################################################################
# Outputs for Redshift Serverless Module
###############################################################################

output "namespace_name" {
  description = "Name of the Redshift Serverless namespace."
  value       = aws_redshiftserverless_namespace.etl.namespace_name
}

output "workgroup_name" {
  description = "Name of the Redshift Serverless workgroup."
  value       = aws_redshiftserverless_workgroup.etl.workgroup_name
}

output "workgroup_arn" {
  description = "ARN of the Redshift Serverless workgroup."
  value       = aws_redshiftserverless_workgroup.etl.arn
}

output "endpoint_address" {
  description = "Endpoint address of the Redshift Serverless workgroup."
  value       = aws_redshiftserverless_workgroup.etl.endpoint[0].address
}

output "secret_arn" {
  description = "ARN of the Secrets Manager secret containing Redshift credentials."
  value       = aws_secretsmanager_secret.redshift.arn
}

output "redshift_copy_role_arn" {
  description = "ARN of the IAM role for Redshift COPY from S3."
  value       = aws_iam_role.redshift_s3.arn
}
