import json
import boto3
import os
import re
from datetime import datetime, timezone

s3 = boto3.client("s3")
sns = boto3.client("sns")
sfn = boto3.client("stepfunctions")


def normalize(name: str) -> str:
    return name.strip().lower()


def sanitize_execution_name(name: str) -> str:
    """
    Sanitize string for Step Function execution name.
    Only allows: alphanumeric, hyphens, underscores.
    Max length: 80 characters.
    """
    # Replace spaces and dots with hyphens
    sanitized = name.replace(' ', '-').replace('.', '-')
    # Remove any character that's not alphanumeric, hyphen, or underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '', sanitized)
    # Ensure it doesn't exceed 80 characters (leaving room for timestamp)
    return sanitized[:50]


def build_glue_job(job, file_name=None):
    return {
        "job_id": job["job_id"],
        "job_name": job["job_name"],
        "source_file_name": file_name or job["source_file_name"],
        "target_table": job["target_table"],
        "upsert_keys": job["upsert_keys"]
    }


def lambda_handler(event, context):

    # ---------------------------------------------------------
    # DEFENSIVE LOGGING (VERY IMPORTANT)
    # ---------------------------------------------------------
    print("🚀 Lambda invoked")
    print(json.dumps(event))

    if event.get("source") != "aws.s3":
        print("Not an EventBridge S3 event → ignoring")
        return {"message": "Ignored non-S3 event"}

    # ---------------------------------------------------------
    # EXTRACT EVENT DETAILS
    # ---------------------------------------------------------
    detail = event.get("detail", {})
    bucket = detail["bucket"]["name"]
    key = detail["object"]["key"]

    print(f"Processing S3 event → bucket={bucket}, key={key}")

    # ---------------------------------------------------------
    # IGNORE STAGING AND ARCHIVE FILES
    # ---------------------------------------------------------
    if key.startswith("data/staging/") or key.startswith("data/archive/"):
        print(f"⏭️  Ignoring file in staging/archive: {key}")
        return {"message": "Ignored - staging/archive file"}

    # ---------------------------------------------------------
    # ENV VARIABLES
    # ---------------------------------------------------------
    CONFIG_BUCKET = os.environ["CONFIG_BUCKET"]
    CONFIG_KEY = os.environ["CONFIG_KEY"]
    SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
    STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]

    # ---------------------------------------------------------
    # LOAD CONFIG.JSON
    # ---------------------------------------------------------
    try:
        config_obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=CONFIG_KEY)
        config_jobs = json.loads(config_obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"Failed to load config.json: {e}")
        raise

    # ---------------------------------------------------------
    # CASE 1: CONFIG.JSON UPLOADED → RE-RUN ALL DATA FILES
    # ---------------------------------------------------------
    if key.startswith("config/") and key.endswith(".json"):

        print("Detected config.json upload → processing all data/in/*.csv")

        # Send simple SNS notification
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Config.json Uploaded",
            Message=f"Config.json has been uploaded.\n\nBucket: {bucket}\nKey: {key}"
        )
        print("📧 Config upload notification sent")

        response = s3.list_objects_v2(Bucket=bucket, Prefix="data/in/")
        if "Contents" not in response:
            print("No data files found under data/in/")
            return {"message": "No data files found"}

        for obj in response["Contents"]:
            if not obj["Key"].endswith(".csv"):
                continue

            file_name = obj["Key"].split("/")[-1]
            normalized_file = normalize(file_name)

            for job in config_jobs:
                # Extract filename from config path
                config_file = normalize(job["source_file_name"].split("/")[-1])

                if normalized_file == config_file and job.get("is_active", False):

                    execution_name = (
                        f"run-{job['job_id']}-"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
                    )

                    sfn.start_execution(
                        stateMachineArn=STEP_FUNCTION_ARN,
                        name=execution_name,
                        input=json.dumps({
                            "glue_jobs": [
                                build_glue_job(job, file_name)
                            ]
                        })
                    )

                    print(f"Triggered job for {file_name} → execution: {execution_name}")

        return {"message": "Config refresh completed"}

    # ---------------------------------------------------------
    # CASE 2: NEW FILE ARRIVED IN data/in/
    # ---------------------------------------------------------
    if key.startswith("data/in/") and key.endswith(".csv"):

        file_name = key.split("/")[-1]
        normalized_file = normalize(file_name)

        print(f"Processing new data file → {file_name}")

        # Find matching job (active or inactive)
        matching_job = None
        for job in config_jobs:
            config_file = normalize(job["source_file_name"].split("/")[-1])
            if config_file == normalized_file:
                matching_job = job
                break

        # ---------------------------------------------------------
        # CASE 2A: FILE NOT IN CONFIG AT ALL
        # ---------------------------------------------------------
        if not matching_job:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="⚠️ Unmapped S3 CSV File Uploaded",
                Message=(
                    f"A CSV file was uploaded but not found in config.json\n\n"
                    f"📁 File Details:\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"• Bucket: {bucket}\n"
                    f"• Key: {key}\n"
                    f"• File Name: {file_name}\n\n"
                    f"⚡ Action Required:\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"1. Add this file to config.json with appropriate job configuration, OR\n"
                    f"2. Remove the file from data/in/ if uploaded by mistake\n\n"
                    f"This is an automated notification from the ETL pipeline."
                )
            )
            print(f"📧 SNS sent for unmapped file → {file_name}")
            return {"message": "SNS sent for unmapped file", "file": file_name}

        # ---------------------------------------------------------
        # CASE 2B: FILE IN CONFIG BUT JOB IS INACTIVE
        # ---------------------------------------------------------
        if not matching_job.get("is_active", False):
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="⏸️ CSV File Upload Skipped - Job Inactive",
                Message=(
                    f"A CSV file was uploaded but its job is marked as inactive in config.json\n\n"
                    f"📁 File Details:\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"• Bucket: {bucket}\n"
                    f"• Key: {key}\n"
                    f"• File Name: {file_name}\n\n"
                    f"📋 Job Details:\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"• Job ID: {matching_job['job_id']}\n"
                    f"• Job Name: {matching_job['job_name']}\n"
                    f"• Target Table: {matching_job['target_table']}\n"
                    f"• Status: INACTIVE\n\n"
                    f"⚡ Action Required:\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Set 'is_active: true' in config.json to process this file.\n\n"
                    f"This is an automated notification from the ETL pipeline."
                )
            )
            print(f"📧 SNS sent for inactive job → {file_name} (job_id: {matching_job['job_id']})")
            return {
                "message": "SNS sent for inactive job",
                "file": file_name,
                "job_id": matching_job["job_id"]
            }

        # ---------------------------------------------------------
        # CASE 2C: FILE IN CONFIG AND JOB IS ACTIVE → TRIGGER
        # ---------------------------------------------------------
        matched_jobs = [build_glue_job(matching_job, file_name)]

        safe_name = sanitize_execution_name(normalized_file)
        execution_name = (
            f"run-{safe_name}-"
            f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        )

        sfn.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            name=execution_name,
            input=json.dumps({
                "glue_jobs": matched_jobs
            })
        )

        print(f"✅ Step Function triggered for {file_name} → execution: {execution_name}")
        return {
            "message": "Job triggered",
            "file": file_name,
            "job_id": matching_job["job_id"],
            "execution_name": execution_name
        }

    # ---------------------------------------------------------
    # CASE 3: EVERYTHING ELSE → IGNORE
    # ---------------------------------------------------------
    print("Ignoring non-relevant S3 event")
    return {"message": "Ignored"}
