#!/usr/bin/env bash
###############################################################################
# Bootstrap Terraform Backend Resources
#
# Creates the S3 buckets and DynamoDB tables required by Terraform's remote
# state backend for each environment (dev, qa, staging, prod).
#
# Prerequisites:
#   - AWS CLI configured with credentials that have S3 and DynamoDB permissions
#   - Run once before the first 'terraform init'
#
# Usage:
#   ./scripts/bootstrap_terraform_backend.sh                  # all environments
#   ./scripts/bootstrap_terraform_backend.sh dev              # single environment
#   ./scripts/bootstrap_terraform_backend.sh dev qa           # specific environments
###############################################################################

set -euo pipefail

REGION="us-east-1"
PROJECT="etl-orchestrator"
ALL_ENVS=("dev" "qa" "staging" "prod")

# Use provided envs or default to all
if [ $# -gt 0 ]; then
    ENVS=("$@")
else
    ENVS=("${ALL_ENVS[@]}")
fi

for ENV in "${ENVS[@]}"; do
    BUCKET="${PROJECT}-tfstate-${ENV}"
    TABLE="${PROJECT}-tflock-${ENV}"

    echo "============================================"
    echo "Bootstrapping backend for: ${ENV}"
    echo "  S3 Bucket:      ${BUCKET}"
    echo "  DynamoDB Table:  ${TABLE}"
    echo "============================================"

    # --- S3 Bucket ---
    if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
        echo "  S3 bucket '${BUCKET}' already exists. Skipping."
    else
        echo "  Creating S3 bucket '${BUCKET}'..."
        if [ "${REGION}" = "us-east-1" ]; then
            aws s3api create-bucket \
                --bucket "${BUCKET}" \
                --region "${REGION}"
        else
            aws s3api create-bucket \
                --bucket "${BUCKET}" \
                --region "${REGION}" \
                --create-bucket-configuration LocationConstraint="${REGION}"
        fi

        # Enable versioning
        aws s3api put-bucket-versioning \
            --bucket "${BUCKET}" \
            --versioning-configuration Status=Enabled

        # Enable server-side encryption
        aws s3api put-bucket-encryption \
            --bucket "${BUCKET}" \
            --server-side-encryption-configuration '{
                "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
            }'

        # Block public access
        aws s3api put-public-access-block \
            --bucket "${BUCKET}" \
            --public-access-block-configuration \
                BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

        echo "  S3 bucket '${BUCKET}' created successfully."
    fi

    # --- DynamoDB Table ---
    if aws dynamodb describe-table --table-name "${TABLE}" --region "${REGION}" >/dev/null 2>&1; then
        echo "  DynamoDB table '${TABLE}' already exists. Skipping."
    else
        echo "  Creating DynamoDB table '${TABLE}'..."
        aws dynamodb create-table \
            --table-name "${TABLE}" \
            --attribute-definitions AttributeName=LockID,AttributeType=S \
            --key-schema AttributeName=LockID,KeyType=HASH \
            --billing-mode PAY_PER_REQUEST \
            --region "${REGION}"

        # Wait for table to be active
        aws dynamodb wait table-exists --table-name "${TABLE}" --region "${REGION}"
        echo "  DynamoDB table '${TABLE}' created successfully."
    fi

    echo ""
done

echo "Bootstrap complete. You can now run 'terraform init' for the bootstrapped environments."
