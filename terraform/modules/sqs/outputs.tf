output "queue_arn" {
  description = "ARN of the ingestion FIFO queue"
  value       = aws_sqs_queue.ingestion.arn
}

output "queue_url" {
  description = "URL of the ingestion FIFO queue"
  value       = aws_sqs_queue.ingestion.url
}

output "queue_name" {
  description = "Name of the ingestion FIFO queue"
  value       = aws_sqs_queue.ingestion.name
}

output "dlq_arn" {
  description = "ARN of the dead-letter FIFO queue"
  value       = aws_sqs_queue.ingestion_dlq.arn
}

output "dlq_url" {
  description = "URL of the dead-letter FIFO queue"
  value       = aws_sqs_queue.ingestion_dlq.url
}
