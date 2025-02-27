import boto3
import logging
import os
import sys
import time
import tempfile
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from botocore.exceptions import ClientError

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

from common import (
    run_command,
    makedir
)

S3_BIN_FOLDER = 'packages'
S3_DOWNLOAD_PATH = '/tmp/'

# NOTE: this should match the environment variables in common/environment.hh
ENV_VARS = [
    'REDIS_USER',
    'REDIS_PASSWORD',
    'REDIS_USER_DATABASE_ID',
    'REDIS_CONFIG_DATABASE_ID',
    'REDIS_PORT',
    'ORGANIZATION_ID',
    'ACCOUNT_ID',
    'DATABASE_INSTANCE_ID',
    'LUSTRE_DNS_NAME',
    'LUSTRE_MOUNT_NAME',
    'MOUNT_POINT',
    'FDW_ID',
    'REPLICATION_USER_PASSWORD',
    'FDW_USER_PASSWORD'
]

SNS_ENV_VARS = [
    'ORGANIZATION_ID',
    'ACCOUNT_ID',
    'DATABASE_INSTANCE_ID',
    'SERVICE_NAME',
    'INSTANCE_KEY',
    'HOSTNAME',
    'FDW_ID'
]

# Cache the SNS topic ARN
TOPIC_ARN = None

# Cache the SNS attributes
SNS_ATTRIBUTES = None

def __download_s3_binaries (
    bucket: str,
    folder: str,
    local_path: str,
    prefix: str = 'springtail-'
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
        send_sns('download_start')

        # List objects with the given prefix
        prefix = f"{folder}/{prefix}" if folder else "{prefix}"
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )

        if 'Contents' not in response or not response['Contents']:
            logger.warning(f"No objects found in {bucket}/{prefix}")
            return None

        files = [obj['Key'] for obj in response['Contents']]

        logger.debug(f"Found {len(response['Contents'])} objects in {prefix}")
        logger.debug(f"Objects: {files}")

        # Sort by the YYYYMMDD portion of filename
        latest_file = sorted(
            files,
            key=lambda x: x.split(prefix)[1].split('.')[0],
            reverse=True
        )[0]

        logger.debug(f"Latest springtail file: {latest_file}")

        # download the file
        filename = os.path.join(local_path, os.path.basename(latest_file))
        s3.download_file(bucket, latest_file, filename)

        send_sns('download_complete', version=latest_file)

        return filename

    except ClientError as e:
        error_code = e.response['Error']['Code']
        logging.error(f"Failed to get latest springtail file: {error_code}")
        print(f"Failed to get latest springtail file: {error_code}")
        send_sns('download_failed')
        sys.exit(1)
        return None
    except Exception as e:
        logging.error(f"Failed to get latest springtail file: {str(e)}")
        print(f"Failed todd get latest springtail file: {str(e)}")
        send_sns('download_failed')
        sys.exit(1)
        return None


def __send_sns_notification(
    topic_arn: str,
    subject: str,
    message: str,
    attributes: Optional[Dict[str, Any]] = {}
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

        if attributes:
            message_attributes = {
                k: {'DataType': 'String', 'StringValue': str(v)}
                for k, v in attributes.items()
            }
        else:
            message_attributes = {}

        sns.publish(TopicArn=topic_arn, Message=message, Subject=subject, MessageAttributes=message_attributes)

        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"Failed to send sns message: {error_code}")
        return False
    except Exception as e:
        logger.error(f"Failed to send sns message: {str(e)}")
        return False


def install_binaries(install_path : str) -> None:
    """
    Install the springtail binaries on the local system.
    Current s3 bucket: s3://data-share.springtail.internal/packages/
    """
    global S3_DOWNLOAD_PATH, S3_BIN_FOLDER

    # Download the springtail binaries
    s3_bucket = os.environ.get('S3_BUCKET',"data-share.springtail.internal")
    if not s3_bucket:
        raise ValueError("S3_BUCKET environment variable not set")

    logging.info(f"Downloading springtail binaries from {s3_bucket}/{S3_BIN_FOLDER} to {S3_DOWNLOAD_PATH}")

    springtail_tgz = __download_s3_binaries(s3_bucket, S3_BIN_FOLDER, S3_DOWNLOAD_PATH)

    if not springtail_tgz:
        raise ValueError("Failed to download springtail binaries")

    try:
        # Create the install directory if it doesn't exist
        if not os.path.exists(install_path):
            makedir(install_path)

        # Install the binaries
        run_command('sudo', ['tar', 'xzf', springtail_tgz, '-C', install_path])

        logging.info(f"Springtail binaries installed to {install_path}")
        send_sns('install_complete', version=os.path.basename(springtail_tgz))

    except Exception as e:
        logging.error(f"Failed to install springtail binaries: {str(e)}")
        send_sns('install_failed', version=os.path.basename(springtail_tgz))
        raise e


def install_pgfdw(install_path : str) -> None:
    """
    Install the postgres libraries on the local system for the FDW.
    Should be done prior to starting the ddl mgr.
    """
    # Get the share and lib directories
    share_dir = run_command('pg_config', ['--sharedir'])
    lib_dir = run_command('pg_config', ['--pkglibdir'])

    # copy the extension files to the share directory
    logging.info(f"Copying extension files to the share directory: {share_dir}")
    sp_sharedir = os.path.join(install_path, 'share')
    share_dir = os.path.join(share_dir.strip(), 'extension')

    run_command('sudo', ['cp', str(os.path.join(sp_sharedir, 'springtail_fdw--1.0.sql')), share_dir])
    run_command('sudo', ['cp', str(os.path.join(sp_sharedir, 'springtail_fdw.control')), share_dir])

    # copy the shared library to the lib directory
    logging.info(f"Copying shared library to the lib directory: {lib_dir}")
    sp_libdir = os.path.join(install_path, 'lib')
    lib_dir = os.path.join(lib_dir.strip(), 'springtail_fdw.so')
    run_command('sudo', ['cp', os.path.join(sp_libdir, 'libspringtail_pg_fdw.so'), lib_dir])

    # Update the postgres configuration file
    # version string is like: 'PostgreSQL 16.4 (Ubuntu 16.4-0ubuntu0.24.04.2)'
    logging.info("Updating postgres environment file")
    version_str = run_command('pg_config', ['--version']).strip()
    version = version_str.split(' ')[1].split('.')[0]
    env_file = f'/etc/postgresql/{version}/main/environment'

    # Write the environment variables to a temporary file
    with tempfile.NamedTemporaryFile(delete=True, mode='w') as temp_file:
        # Write data to the temporary file
        for var in ENV_VARS:
            value = os.environ.get(var)
            if value:
                temp_file.write(f"{var} = '{value}'\n")
        temp_file.flush()

        # Copy the contents of the temporary file to the environment file
        run_command('sudo', ['cp', temp_file.name, env_file])

    # restart postgres
    logging.info("Restarting postgres")
    run_command('sudo', ['service', 'postgresql', 'restart'])
    time.sleep(5)

def _extract_attributes() -> Dict[str, Any]:
    """
    Extract attributes from environment variables.
    """
    global SNS_ENV_VARS

    attributes = {}

    # extract attributes from environment variables
    for var in SNS_ENV_VARS:
        value = os.environ.get(var)
        if value:
            attributes[var] = value

    # get aws instance id
    token = run_command('curl', ['-s', 'http://169.254.169.254/latest/api/token', '-X', 'PUT', '-H', 'X-aws-ec2-metadata-token-ttl-seconds: 21600'])
    instance_id = run_command('curl', ['-s', 'http://169.254.169.254/latest/meta-data/instance-id', '-H', f'X-aws-ec2-metadata-token: {token}'])

    attributes['AWS_INSTANCE_ID'] = instance_id

    # generate SRN: format: srn:1:1:aws:dbi/82
    srn = f"srn:{attributes['ORGANIZATION_ID']}:{attributes['ACCOUNT_ID']}:aws:dbi/{attributes['DATABASE_INSTANCE_ID']}"
    attributes['SRN'] = srn

    return {k.lower(): v for k, v in attributes.items()}


def send_sns(
    type: str,
    component: Optional[str] = None,
    version: Optional[str] = None
) -> None:
    """
    Send a message to the SNS topic.
    """

    global TOPIC_ARN, SNS_ATTRIBUTES
    if not TOPIC_ARN:
        TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')
        if not TOPIC_ARN:
            return

    if not SNS_ATTRIBUTES:
        SNS_ATTRIBUTES = _extract_attributes()

    srn = SNS_ATTRIBUTES['srn']
    service_name = SNS_ATTRIBUTES['service_name']

    now = datetime.now(timezone.utc)
    timestamp_ms = int(now.timestamp() * 1000)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # copy the attributes and add the timestamp
    attributes = dict(SNS_ATTRIBUTES)
    attributes['epoch_ms'] = timestamp_ms
    attributes['timestamp'] = timestamp
    attributes['source'] = 'coordinator'

    if type == 'startup':
        subject = f"Instance startup: {srn}, {service_name} @{timestamp}"
    elif type == 'shutdown':
        subject = f"Instance shutdown: {srn}, {service_name} @{timestamp}"
    elif type == 'failure':
        subject = f"Failure detected: {srn}, {service_name}, {component} @{timestamp}"
    elif type == 'download_start':
        subject = f"New version downloading: {srn}, {service_name} @{timestamp}"
    elif type == 'download_complete':
        subject = f"New version downloaded: {srn}, {service_name} @{timestamp}"
        attributes['version'] = version
    elif type == 'download_failed':
        subject = f"New version download failed: {srn}, {service_name} @{timestamp}"
    elif type == 'install_complete':
        subject = f"New version installed: {srn}, {service_name} @{timestamp}"
        attributes['version'] = version
    elif type == 'install_failed':
        subject = f"New version install failed: {srn}, {service_name} @{timestamp}"
        attributes['version'] = version
    else:
        logging.error(f"Unknown SNS message type: {type}")
        return

    message = f"{subject}\n\n{json.dumps(attributes)}"

    __send_sns_notification(TOPIC_ARN, subject, message, attributes)
