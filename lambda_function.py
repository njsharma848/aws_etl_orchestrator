import json
import boto3
import os
import re
from datetime import datetime, timezone
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
sns = boto3.client("sns")
sfn = boto3.client("stepfunctions")


def normalize(name):
    """Convert filename to lowercase for comparison"""
    if not name:
        return ""
    return name.strip().lower()


def sanitize_execution_name(name):
    """Clean filename for use in Step Functions execution name"""
    sanitized = name.replace(' ', '-').replace('.', '-')
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '', sanitized)
    return sanitized[:50]


def build_glue_job(job, file_name=None):
    """Prepare job configuration for Step Functions"""
    return {
        "job_id": job["job_id"],
        "job_name": job["job_name"],
        "source_file_name": file_name or job["source_file_name"],
        "target_table": job["target_table"],
        "upsert_keys": job["upsert_keys"]
    }


def load_all_config_files(bucket, config_prefix="config/"):
    """Load and merge all config files from S3"""
    all_jobs = []
    
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=config_prefix)
        
        if "Contents" not in response:
            print(f"Warning: No config files found in {config_prefix}")
            return []
        
        for obj in response["Contents"]:
            config_key = obj["Key"]
            
            if config_key.endswith('/') or not config_key.endswith(".json"):
                continue
            
            try:
                print(f"Loading config: {config_key}")
                config_obj = s3.get_object(Bucket=bucket, Key=config_key)
                config_jobs = json.loads(config_obj["Body"].read().decode("utf-8"))
                
                if not isinstance(config_jobs, list):
                    print(f"Skipping {config_key} - invalid format")
                    continue
                
                # Track which config file each job came from
                for job in config_jobs:
                    job["_config_source"] = config_key
                
                all_jobs.extend(config_jobs)
                print(f"Loaded {len(config_jobs)} jobs from {config_key}")
                
            except (ClientError, json.JSONDecodeError) as e:
                print(f"Error loading {config_key}: {e}")
                continue
        
        print(f"Total jobs loaded: {len(all_jobs)}")
        return all_jobs
        
    except ClientError as e:
        print(f"Failed to list config files: {e}")
        raise


def send_notification(topic_arn, subject, message):
    """Send email notification via SNS"""
    try:
        sns.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message
        )
        print(f"Notification sent: {subject}")
        return True
    except ClientError as e:
        print(f"Failed to send notification: {e}")
        return False


def lambda_handler(event, context):
    """Process S3 file upload events"""
    
    print("Lambda triggered")
    print(json.dumps(event))

    # Verify this is an S3 event
    if event.get("source") != "aws.s3":
        print("Ignoring non-S3 event")
        return {"message": "Not an S3 event"}

    # Get file details from event
    try:
        detail = event.get("detail", {})
        bucket = detail["bucket"]["name"]
        key = detail["object"]["key"]
    except KeyError as e:
        print(f"Invalid event structure: {e}")
        return {"message": "Invalid event"}

    print(f"Processing: {key} from {bucket}")

    # Skip config files - they're loaded dynamically
    if key.startswith("config/"):
        print("Ignoring config file upload")
        return {"message": "Config file ignored"}
    
    # Skip staging and archive folders
    if key.startswith("data/staging/") or key.startswith("data/archive/"):
        print("Ignoring staging/archive file")
        return {"message": "Staging/archive file ignored"}

    # Only process CSV files in data/in/
    if not key.startswith("data/in/") or not key.endswith(".csv"):
        print("File not in data/in/ or not CSV")
        return {"message": "File ignored"}

    # Get environment variables
    try:
        SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
        STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]
    except KeyError as e:
        print(f"Missing environment variable: {e}")
        raise

    # Load config files
    config_jobs = load_all_config_files(bucket)
    
    if not config_jobs:
        file_name = key.split("/")[-1]
        send_notification(
            SNS_TOPIC_ARN,
            "No Config Files Found",
            f"File uploaded but no configuration found.\n\n"
            f"File: {file_name}\n"
            f"Bucket: {bucket}\n"
            f"Key: {key}\n\n"
            f"Please add config files to the config/ folder."
        )
        return {"message": "No config files"}

    # Extract filename
    file_name = key.split("/")[-1]
    normalized_file = normalize(file_name)

    print(f"Processing file: {file_name}")

    # Find matching job configuration
    matching_job = None
    for job in config_jobs:
        config_file = normalize(job["source_file_name"].split("/")[-1])
        if config_file == normalized_file:
            matching_job = job
            print(f"Matched with job: {job['job_id']}")
            break

    # Handle unmapped file
    if not matching_job:
        print("No matching job found")
        
        send_notification(
            SNS_TOPIC_ARN,
            "Unmapped File Uploaded",
            f"File uploaded but not configured for processing.\n\n"
            f"File: {file_name}\n"
            f"Bucket: {bucket}\n"
            f"Key: {key}\n\n"
            f"Action needed:\n"
            f"- Add configuration for this file, or\n"
            f"- Remove file if uploaded by mistake\n\n"
            f"Searched {len(config_jobs)} job configs."
        )
        
        return {"message": "File not configured"}

    # Handle inactive job
    if not matching_job.get("is_active", False):
        print("Job is inactive")
        
        send_notification(
            SNS_TOPIC_ARN,
            "File Upload Skipped - Inactive Job",
            f"File uploaded but job is currently inactive.\n\n"
            f"File: {file_name}\n"
            f"Job ID: {matching_job['job_id']}\n"
            f"Job Name: {matching_job['job_name']}\n"
            f"Config: {matching_job.get('_config_source', 'unknown')}\n\n"
            f"To process this file, set is_active to true in the config."
        )
        
        return {"message": "Job inactive"}

    # Trigger Step Functions
    safe_name = sanitize_execution_name(normalized_file)
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    execution_name = f"run-{safe_name}-{timestamp}"

    print(f"Starting execution: {execution_name}")

    try:
        sfn.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            name=execution_name,
            input=json.dumps({
                "glue_jobs": [build_glue_job(matching_job, file_name)]
            })
        )
        
        print("Step Function started successfully")
        
        return {
            "message": "Job triggered",
            "file": file_name,
            "job_id": matching_job["job_id"],
            "execution": execution_name
        }
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ExecutionAlreadyExists':
            print("Execution already exists")
            return {"message": "Already running"}
        else:
            print(f"Failed to start Step Function: {e}")
            raise
