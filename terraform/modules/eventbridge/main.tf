################################################################################
# EventBridge Rule - S3 Object Created Trigger
################################################################################

resource "aws_cloudwatch_event_rule" "s3_ingestion_trigger" {
  name        = "${var.project_name}-${var.environment}-s3-ingestion-trigger"
  description = "Triggers on S3 object creation in the data/in/ prefix"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.s3_bucket_name]
      }
      object = {
        key = [{
          prefix = "data/in/"
        }]
      }
    }
  })

  tags = var.tags
}

################################################################################
# EventBridge Target - SQS FIFO Queue
#
# Events flow: S3 -> EventBridge -> SQS FIFO -> Lambda (concurrency=1)
# This replaces the direct EventBridge -> Lambda invocation to provide:
#   - Buffering for burst file uploads
#   - Guaranteed delivery (messages retained up to 4 days)
#   - DLQ for poison messages
#   - Concurrency control via Lambda reserved concurrency
################################################################################

resource "aws_cloudwatch_event_target" "sqs_ingestion" {
  rule = aws_cloudwatch_event_rule.s3_ingestion_trigger.name
  arn  = var.sqs_queue_arn

  sqs_target {
    message_group_id = "ingestion-events"
  }
}
