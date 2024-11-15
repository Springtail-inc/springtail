import boto3
import logging
import os
import sys
from typing import Optional, Dict, Any
from botocore.exceptions import ClientError

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

from utils import run_command

S3_BIN_FOLDER = 'packages'
S3_DOWNLOAD_PATH = '/tmp'
S3_INSTALL_PATH = '/opt/'

def __download_s3_binaries (
    bucket: str,
    folder: str,
    local_path: str,
    prefix: str = 'springtail_'
) -> Optional[str]:
    """
    Get the latest springtail binaries file from S3 based on filename timestamp.

    Args:
        bucket: S3 bucket name
        folder: Folder prefix in bucket (no leading/trailing slash needed)
        local_path: Local path to download the file
        prefix: Prefix to filter files by (default: 'springtail_')

    Returns:
        str: path to the downloaded file, None if failed
    """
    s3 = boto3.client('s3')
    logger = logging.getLogger("S3")

    try:
        # List objects with the given prefix
        prefix = f"{folder}/" if folder else ""
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )

        if 'Contents' not in response:
            return None

        # Filter for springtail backup files and sort by timestamp in filename
        files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].startswith('springtail_')
        ]

        if not files:
            return None

        # Sort by the YYYYMMDD portion of filename
        latest_file = sorted(
            files,
            key=lambda x: x.split(prefix)[1].split('.')[0],
            reverse=True
        )[0]

        # download the file
        s3.download_file(bucket, latest_file, local_path)

        return os.path.join(local_path, latest_file)

    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Failed to get latest springtail file: {error_code}")
        return None
    except Exception as e:
        logger.error(f"Failed to get latest springtail file: {str(e)}")
        return None


def __send_sns_notification(
    topic_arn: str,
    message: str,
    subject: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Send a notification to an SNS topic.

    Args:
        topic_arn: The ARN of the SNS topic
        message: The message to send
        subject: Optional subject line (useful for email subscriptions)
        attributes: Optional message attributes

    Returns:
        bool: True if message was sent successfully, False otherwise
    """
    sns = boto3.client('sns')
    logger = logging.getLogger("SNS")

    try:
        kwargs = {
            'TopicArn': topic_arn,
            'Message': message
        }

        if subject:
            kwargs['Subject'] = subject

        if attributes:
            kwargs['MessageAttributes'] = attributes

        sns.publish(**kwargs)
        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Failed to send sns message: {error_code}")
        return None
    except Exception as e:
        logger.error(f"Failed to send sns message: {str(e)}")
        return None


def install_binaries() -> None:
    """
    Install the springtail binaries on the local system.
    """
    # Download the springtail binaries
    s3_bucket = os.environ.get('S3_BUCKET')
    if not s3_bucket:
        raise ValueError("S3_BUCKET environment variable not set")

    springtail_tgz = __download_s3_binaries(s3_bucket, S3_BIN_FOLDER, S3_DOWNLOAD_PATH)

    if not springtail_tgz:
        raise ValueError("Failed to download springtail binaries")

    # Install the binaries
    run_command('tar', ['-xzf', springtail_tgz, '-C', S3_INSTALL_PATH])

def send_sns(message: str) -> None:
    """
    Send a message to the SNS topic.
    """
    topic_arn = os.environ.get('SNS_TOPIC_ARN')
    if not topic_arn:
        raise ValueError("SNS_TOPIC_ARN environment variable not set")

    __send_sns_notification(topic_arn, message)
