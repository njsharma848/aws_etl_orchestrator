import boto3
import paramiko
import json
from datetime import datetime, timezone
import os


def lambda_handler(event, context):
    """
    Triggered by S3 event when log file is created
    Transfers log file to SFTP immediately
    """

    SECRET_NAME = os.environ.get('SFTP_SECRET_NAME', 'prod/sftp/log-transfer')

    s3_client = boto3.client('s3')
    secrets_client = boto3.client('secretsmanager')

    try:
        # Parse S3 event
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        log_file_key = record['s3']['object']['key']

        filename = log_file_key.split('/')[-1]

        print(f"Processing: {log_file_key}")

        # Skip non-log files
        if not log_file_key.startswith('logs/') or not filename.endswith('.txt'):
            print(f"Skipping non-log file: {log_file_key}")
            return {'statusCode': 200, 'body': 'Skipped'}

        # Download from S3
        log_obj = s3_client.get_object(Bucket=bucket, Key=log_file_key)
        log_content = log_obj['Body'].read()

        print(f"Downloaded {filename}: {len(log_content)} bytes")

        # Get SFTP credentials (will fail if not configured - that's OK)
        secret = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        creds = json.loads(secret['SecretString'])

        # Connect to SFTP
        print(f"Connecting to SFTP: {creds['sftp_host']}:{creds.get('sftp_port', 22)}")

        ssh = paramiko.SSHClient()

        # Use known host key from Secrets Manager if available, otherwise reject
        if creds.get('host_key'):
            host_key_data = paramiko.RSAKey(data=paramiko.py3compat.decodebytes(
                creds['host_key'].encode('utf-8')
            ))
            ssh.get_host_keys().add(creds['sftp_host'], 'ssh-rsa', host_key_data)
            ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
            # Fallback to WarningPolicy when host key is not configured
            ssh.set_missing_host_key_policy(paramiko.WarningPolicy())

        ssh.connect(
            hostname=creds['sftp_host'],
            port=int(creds.get('sftp_port', 22)),
            username=creds['username'],
            password=creds['password'],
            timeout=30,
            banner_timeout=30
        )

        sftp = ssh.open_sftp()

        # Create date-based directory structure on SFTP
        # Extract date from path: logs/2025/12/17/file.txt
        path_parts = log_file_key.split('/')
        if len(path_parts) >= 4:
            year, month, day = path_parts[1], path_parts[2], path_parts[3]
            date_folder = f"{year}{month}{day}"
            remote_dir = f"{creds['remote_dir']}/{date_folder}"

            # Create directory if needed
            try:
                sftp.stat(remote_dir)
                print(f"Directory exists: {remote_dir}")
            except IOError:
                try:
                    sftp.mkdir(remote_dir)
                    print(f"Created directory: {remote_dir}")
                except IOError:
                    # Concurrent Lambda might have created it
                    print(f"Directory creation skipped (likely exists): {remote_dir}")
        else:
            remote_dir = creds['remote_dir']

        # Upload to SFTP
        remote_path = f"{remote_dir}/{filename}"

        print(f"Uploading to: {remote_path}")

        with sftp.file(remote_path, 'wb') as remote_file:
            remote_file.write(log_content)

        sftp.close()
        ssh.close()

        print(f"Successfully transferred: {filename} ({len(log_content)} bytes)")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'file': filename,
                'size': len(log_content),
                's3_path': log_file_key,
                'remote_path': remote_path,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

    except Exception as e:
        # Log the error with full details
        print(f"Error transferring {filename if 'filename' in locals() else 'file'}: {str(e)}")
        import traceback
        traceback.print_exc()

        # Re-raise to trigger Lambda's automatic retry mechanism
        raise
