################################################################################
# SQS FIFO Queue - Ingestion event buffer with concurrency control
################################################################################

resource "aws_sqs_queue" "ingestion_dlq" {
  name                        = "${var.project_name}-${var.environment}-ingestion-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  message_retention_seconds   = 1209600 # 14 days
  tags                        = var.tags
}

resource "aws_sqs_queue" "ingestion" {
  name                        = "${var.project_name}-${var.environment}-ingestion.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  visibility_timeout_seconds  = 360    # 6x Lambda timeout (60s)
  message_retention_seconds   = 345600 # 4 days
  receive_wait_time_seconds   = 20     # long polling

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.ingestion_dlq.arn
    maxReceiveCount     = 3
  })

  tags = var.tags
}

# Allow EventBridge to send messages to the FIFO queue
resource "aws_sqs_queue_policy" "allow_eventbridge" {
  queue_url = aws_sqs_queue.ingestion.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowEventBridgeSendMessage"
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.ingestion.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = var.eventbridge_rule_arn
          }
        }
      }
    ]
  })
}
