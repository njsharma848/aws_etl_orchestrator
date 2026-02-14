###############################################################################
# Redshift Serverless - Namespace, Workgroup, Secret, and IAM Role
###############################################################################

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# -----------------------------------------------------------------------------
# Auto-generated admin password (stored in Secrets Manager)
# -----------------------------------------------------------------------------
resource "random_password" "admin" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# -----------------------------------------------------------------------------
# IAM Role for Redshift to COPY from S3
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "redshift_assume" {
  statement {
    sid     = "RedshiftAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_s3" {
  name               = "${var.project_name}-${var.environment}-redshift-s3-access"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "redshift_s3" {
  statement {
    sid    = "S3ReadAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [
      var.s3_bucket_arn,
      "${var.s3_bucket_arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "redshift_s3" {
  name   = "${var.project_name}-${var.environment}-redshift-s3-policy"
  role   = aws_iam_role.redshift_s3.id
  policy = data.aws_iam_policy_document.redshift_s3.json
}

# -----------------------------------------------------------------------------
# Redshift Serverless Namespace (database + credentials)
# -----------------------------------------------------------------------------
resource "aws_redshiftserverless_namespace" "etl" {
  namespace_name      = "${var.project_name}-${var.environment}-namespace"
  db_name             = var.database_name
  admin_username      = var.admin_username
  admin_user_password = random_password.admin.result
  iam_roles           = [aws_iam_role.redshift_s3.arn]
  tags                = var.tags
}

# -----------------------------------------------------------------------------
# Redshift Serverless Workgroup (compute)
# -----------------------------------------------------------------------------
resource "aws_redshiftserverless_workgroup" "etl" {
  namespace_name      = aws_redshiftserverless_namespace.etl.namespace_name
  workgroup_name      = var.workgroup_name
  base_capacity       = var.base_capacity
  publicly_accessible = false
  tags                = var.tags
}

# -----------------------------------------------------------------------------
# Secrets Manager - Store Redshift credentials for Glue Data API access
# -----------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "redshift" {
  name                    = "${var.environment}/redshift/credentials"
  recovery_window_in_days = 0
  tags = merge(var.tags, {
    RedshiftDataFullAccess = aws_redshiftserverless_workgroup.etl.workgroup_name
  })
}

resource "aws_secretsmanager_secret_version" "redshift" {
  secret_id = aws_secretsmanager_secret.redshift.id
  secret_string = jsonencode({
    username = var.admin_username
    password = random_password.admin.result
    host     = aws_redshiftserverless_workgroup.etl.endpoint[0].address
    port     = tostring(aws_redshiftserverless_workgroup.etl.endpoint[0].port)
    database = var.database_name
  })
}
