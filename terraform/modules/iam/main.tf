###############################################################################
# IAM Roles for AWS ETL Pipeline
# Roles: Lambda Orchestrator, Lambda SFTP, Glue ETL, Step Functions
###############################################################################

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# -----------------------------------------------------------------------------
# (a) Lambda Orchestrator Role
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    sid     = "LambdaAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_orchestrator" {
  name               = "${var.project_name}-${var.environment}-lambda-orchestrator"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "lambda_orchestrator" {
  # S3 read on config/ and data/ prefixes
  statement {
    sid    = "S3Read"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.s3_bucket_arn,
      "${var.s3_bucket_arn}/config/*",
      "${var.s3_bucket_arn}/data/*",
    ]
  }

  # S3 write/delete on data/ prefix
  statement {
    sid    = "S3WriteDelete"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${var.s3_bucket_arn}/data/*",
    ]
  }

  # SNS publish
  statement {
    sid    = "SNSPublish"
    effect = "Allow"
    actions = [
      "sns:Publish",
    ]
    resources = [
      var.sns_topic_arn,
    ]
  }

  # Step Functions start execution
  statement {
    sid    = "StepFunctionsStart"
    effect = "Allow"
    actions = [
      "states:StartExecution",
    ]
    resources = [
      var.step_function_arn,
    ]
  }

  # CloudWatch Logs
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/${var.project_name}-${var.environment}-orchestrator:*",
    ]
  }
}

resource "aws_iam_role_policy" "lambda_orchestrator" {
  name   = "${var.project_name}-${var.environment}-lambda-orchestrator-policy"
  role   = aws_iam_role.lambda_orchestrator.id
  policy = data.aws_iam_policy_document.lambda_orchestrator.json
}

# -----------------------------------------------------------------------------
# (b) Lambda SFTP Role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "lambda_sftp" {
  name               = "${var.project_name}-${var.environment}-lambda-sftp"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "lambda_sftp" {
  # S3 read on logs/ prefix
  statement {
    sid    = "S3ReadLogs"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.s3_bucket_arn,
      "${var.s3_bucket_arn}/logs/*",
    ]
  }

  # Secrets Manager read for SFTP secret
  statement {
    sid    = "SecretsManagerRead"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    resources = [
      var.secret_arn,
    ]
  }

  # CloudWatch Logs
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/${var.project_name}-${var.environment}-sftp:*",
    ]
  }
}

resource "aws_iam_role_policy" "lambda_sftp" {
  name   = "${var.project_name}-${var.environment}-lambda-sftp-policy"
  role   = aws_iam_role.lambda_sftp.id
  policy = data.aws_iam_policy_document.lambda_sftp.json
}

# -----------------------------------------------------------------------------
# (c) Glue Job Role
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    sid     = "GlueAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_etl" {
  name               = "${var.project_name}-${var.environment}-glue-etl"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = var.tags
}

# Attach the standard AWS managed policy for Glue
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_etl.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_etl" {
  # S3 read/write on the data bucket
  statement {
    sid    = "S3ReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      var.s3_bucket_arn,
      "${var.s3_bucket_arn}/*",
    ]
  }

  # Redshift Data API
  statement {
    sid    = "RedshiftDataAPI"
    effect = "Allow"
    actions = [
      "redshift-data:ExecuteStatement",
      "redshift-data:DescribeStatement",
      "redshift-data:GetStatementResult",
    ]
    resources = [
      var.redshift_workgroup_arn,
    ]
  }

  # Secrets Manager read
  statement {
    sid    = "SecretsManagerRead"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    resources = [
      var.secret_arn,
    ]
  }

  # CloudWatch Logs
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_etl" {
  name   = "${var.project_name}-${var.environment}-glue-etl-policy"
  role   = aws_iam_role.glue_etl.id
  policy = data.aws_iam_policy_document.glue_etl.json
}

# -----------------------------------------------------------------------------
# (d) Step Functions Orchestrator Role
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    sid     = "StepFunctionsAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_orchestrator" {
  name               = "${var.project_name}-${var.environment}-sfn-orchestrator"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "sfn_orchestrator" {
  # Glue job management
  statement {
    sid    = "GlueJobRun"
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:job/${var.project_name}-${var.environment}-*",
    ]
  }

  # CloudWatch Logs
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "*",
    ]
  }
}

resource "aws_iam_role_policy" "sfn_orchestrator" {
  name   = "${var.project_name}-${var.environment}-sfn-orchestrator-policy"
  role   = aws_iam_role.sfn_orchestrator.id
  policy = data.aws_iam_policy_document.sfn_orchestrator.json
}
