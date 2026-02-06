################################################################################
# EventBridge Module - Outputs
################################################################################

output "event_rule_arn" {
  description = "ARN of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.s3_ingestion_trigger.arn
}

output "event_rule_name" {
  description = "Name of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.s3_ingestion_trigger.name
}
