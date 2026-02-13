import json
import boto3
import os
import re
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# AWS clients
s3 = boto3.client("s3")
sns = boto3.client("sns")
sfn = boto3.client("stepfunctions")

# Environment variables
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]
GLUE_JOB_SIMPLE = os.environ["GLUE_JOB_SIMPLE"]
GLUE_JOB_DATA_MODEL = os.environ["GLUE_JOB_DATA_MODEL"]

CONFIG_KEY = "config/config.json"


# -------------------------------------------------
# Utility helpers
# -------------------------------------------------

def normalize(text):
    return text.strip().lower() if text else ""


def sanitize(text):
    text = text.replace(" ", "-").replace("/", "-").replace(".", "-")
    return re.sub(r"[^a-zA-Z0-9_-]", "", text)[:50]


# -------------------------------------------------
# AWS helpers (HOW)
# -------------------------------------------------

def publish_notification(subject, message):
    """Best-effort SNS publish"""
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
    except Exception as error:
        print(f"SNS publish failed: {error}")


def move_s3_object(bucket_name, source_key, destination_key):
    """Move object within same bucket"""
    s3.copy_object(
        Bucket=bucket_name,
        CopySource={"Bucket": bucket_name, "Key": source_key},
        Key=destination_key
    )
    s3.delete_object(Bucket=bucket_name, Key=source_key)


def resolve_glue_job_name(job_config):
    """Select the Glue job based on the 'glue_job' field in config.json."""
    glue_job = job_config.get("glue_job", "simple")
    if glue_job == "data_model":
        return GLUE_JOB_DATA_MODEL
    return GLUE_JOB_SIMPLE


def start_state_machine_execution(file_name, job_config):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    execution_name = f"run-{sanitize(file_name)}-{timestamp}"
    glue_job_name = resolve_glue_job_name(job_config)

    print(f"Routing to Glue job: {glue_job_name}")

    try:
        sfn.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            name=execution_name,
            input=json.dumps({
                "glue_jobs": [
                    {
                        "glue_job_name": glue_job_name,
                        "job_id": job_config["job_id"],
                        "job_name": job_config["job_name"],
                        "source_file_name": file_name,
                        "target_table": job_config["target_table"],
                        "upsert_keys": job_config["upsert_keys"]
                    }
                ]
            })
        )
        return {"status": "triggered", "execution": execution_name}

    except ClientError as error:
        if error.response["Error"]["Code"] == "ExecutionAlreadyExists":
            return {"status": "already_running"}
        raise


# -------------------------------------------------
# Config helpers
# -------------------------------------------------

def load_job_configurations(bucket_name):
    response = s3.get_object(Bucket=bucket_name, Key=CONFIG_KEY)
    job_configurations = json.loads(response["Body"].read())

    if not isinstance(job_configurations, list):
        raise Exception("config.json must be a list of job definitions")

    return job_configurations


def find_matching_job(job_configurations, s3_object_key):
    uploaded_file_name = normalize(s3_object_key.split("/")[-1])

    return next(
        (
            job_config
            for job_config in job_configurations
            if normalize(
                job_config.get("source_file_name", "").split("/")[-1]
            ) == uploaded_file_name
        ),
        None
    )


# -------------------------------------------------
# Event parsing helpers
# -------------------------------------------------

def _extract_s3_event(event):
    """
    Parse the S3 event detail from either:
      - SQS trigger (EventBridge event wrapped in SQS message body)
      - Direct EventBridge invocation (backward-compatible)
    """
    if "Records" in event:
        # SQS-triggered: EventBridge event is JSON-encoded in the message body
        sqs_record = event["Records"][0]
        eb_event = json.loads(sqs_record["body"])
    else:
        # Direct EventBridge invocation (fallback / local testing)
        eb_event = event

    bucket_name = eb_event["detail"]["bucket"]["name"]
    object_key = eb_event["detail"]["object"]["key"]
    return bucket_name, object_key


# -------------------------------------------------
# Lambda entry point (WHEN & WHY)
# -------------------------------------------------

def lambda_handler(event, context):

    bucket_name, object_key = _extract_s3_event(event)
    file_name = object_key.split("/")[-1]

    print(f"Processing file: {file_name}")

    # Load config
    job_configurations = load_job_configurations(bucket_name)

    # Match job
    matching_job = find_matching_job(job_configurations, object_key)

    # -------------------------------------------------
    # Case 1: NOT CONFIGURED → move + SNS
    # -------------------------------------------------
    if not matching_job:
        destination_key = f"data/new_files/{file_name}"

        move_s3_object(bucket_name, object_key, destination_key)

        publish_notification(
            subject="New / Not Configured File",
            message=(
                f"File not found in config.json\n"
                f"Original: {object_key}\n"
                f"Moved to: {destination_key}"
            )
        )

        return {
            "status": "not_configured",
            "moved_to": destination_key
        }

    # -------------------------------------------------
    # Case 2: CONFIGURED but INACTIVE → SNS
    # -------------------------------------------------
    if not matching_job.get("is_active"):
        publish_notification(
            subject="Inactive Job",
            message=(
                f"File: {file_name}\n"
                f"Job ID: {matching_job.get('job_id')}"
            )
        )

        return {
            "status": "inactive",
            "job_id": matching_job.get("job_id")
        }

    # -------------------------------------------------
    # Case 3: CONFIGURED & ACTIVE → trigger workflow
    # -------------------------------------------------
    result = start_state_machine_execution(file_name, matching_job)

    print(f"Step Function triggered: {result}")

    return result
