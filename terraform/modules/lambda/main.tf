###############################################################################
# Orchestrator Lambda
###############################################################################

data "archive_file" "orchestrator" {
  type        = "zip"
  source_dir  = var.orchestrator_source_dir
  output_path = "${path.module}/builds/orchestrator.zip"
}

resource "aws_lambda_function" "orchestrator" {
  function_name    = "${var.project_name}-${var.environment}-orchestrator"
  role             = var.lambda_orchestrator_role_arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  timeout          = 60
  memory_size      = 256
  filename         = data.archive_file.orchestrator.output_path
  source_code_hash = data.archive_file.orchestrator.output_base64sha256

  environment {
    variables = {
      SNS_TOPIC_ARN     = var.sns_topic_arn
      STEP_FUNCTION_ARN = var.step_function_arn
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "orchestrator" {
  name              = "/aws/lambda/${aws_lambda_function.orchestrator.function_name}"
  retention_in_days = 30
  tags              = var.tags
}

# SQS FIFO -> Lambda event source mapping (batch_size=1 for one-at-a-time)
resource "aws_lambda_event_source_mapping" "sqs_to_orchestrator" {
  event_source_arn                   = var.sqs_queue_arn
  function_name                      = aws_lambda_function.orchestrator.arn
  batch_size                         = 1
  maximum_batching_window_in_seconds = 0
  enabled                            = true
}

###############################################################################
# SFTP Log Transfer Lambda
###############################################################################

data "archive_file" "sftp_log_transfer" {
  type        = "zip"
  source_dir  = var.sftp_source_dir
  output_path = "${path.module}/builds/sftp_log_transfer.zip"
}

resource "aws_lambda_function" "sftp_log_transfer" {
  function_name    = "${var.project_name}-${var.environment}-log-transfer"
  role             = var.lambda_sftp_role_arn
  handler          = "logs_to_smb.lambda_handler"
  runtime          = "python3.12"
  timeout          = 120
  memory_size      = 256
  filename         = data.archive_file.sftp_log_transfer.output_path
  source_code_hash = data.archive_file.sftp_log_transfer.output_base64sha256

  environment {
    variables = {
      S3_BUCKET        = var.s3_bucket_name
      SFTP_SECRET_NAME = var.sftp_secret_name
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "sftp_log_transfer" {
  name              = "/aws/lambda/${aws_lambda_function.sftp_log_transfer.function_name}"
  retention_in_days = 30
  tags              = var.tags
}

# Allow S3 to invoke the SFTP Log Transfer Lambda
resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sftp_log_transfer.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.s3_bucket_arn
}

# S3 bucket notification to trigger on logs/ prefix ObjectCreated events
resource "aws_s3_bucket_notification" "sftp_log_trigger" {
  bucket = var.s3_bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.sftp_log_transfer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "logs/"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}
