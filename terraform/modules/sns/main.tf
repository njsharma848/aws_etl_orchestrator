################################################################################
# SNS Topic - ETL Notifications
################################################################################

resource "aws_sns_topic" "etl_notifications" {
  name              = "${var.project_name}-${var.environment}-etl-notifications"
  kms_master_key_id = "alias/aws/sns"

  tags = var.tags
}

################################################################################
# SNS Email Subscription (conditional)
################################################################################

resource "aws_sns_topic_subscription" "email" {
  count     = var.notification_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.etl_notifications.arn
  protocol  = "email"
  endpoint  = var.notification_email
}
