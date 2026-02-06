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
# EventBridge Target - Lambda Orchestrator
################################################################################

resource "aws_cloudwatch_event_target" "lambda_orchestrator" {
  rule = aws_cloudwatch_event_rule.s3_ingestion_trigger.name
  arn  = var.lambda_orchestrator_arn
}

################################################################################
# Lambda Permission - Allow EventBridge Invocation
################################################################################

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_orchestrator_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.s3_ingestion_trigger.arn
}
