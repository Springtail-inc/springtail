import boto3
import logging
import json
import os
from typing import Optional, Dict, Any, Callable
from botocore.exceptions import ClientError

from common import run_command

class AwsHelper:

    def __init__(self):
        self.logger = logging.getLogger('springtail')
        self.s3 = boto3.client('s3')
        self.sns = boto3.client('sns', region_name='us-east-1')


    def get_instance_id(self) -> Optional[str]:
        """
        Get the instance ID of the current EC2 instance.
        """
        token = run_command('curl', ['-s', 'http://169.254.169.254/latest/api/token', '-X', 'PUT', '-H', 'X-aws-ec2-metadata-token-ttl-seconds: 21600'])
        instance_id = run_command('curl', ['-s', 'http://169.254.169.254/latest/meta-data/instance-id', '-H', f'X-aws-ec2-metadata-token: {token}'])
        return instance_id


    def s3_download(
        self,
        bucket: str,
        folder: str,
        local_path: str,
        prefix: str = 'springtail-',
        sort_func: Optional[Callable] = None
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
        try:
            # List objects with the given prefix
            prefix = f"{folder}/{prefix}" if folder else "{prefix}"
            response = self.s3.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )

            if 'Contents' not in response or not response['Contents']:
                self.logger.warning(f"No objects found in {bucket}/{prefix}")
                return None

            files = [obj['Key'] for obj in response['Contents']]

            self.logger.debug(f"Found {len(response['Contents'])} objects in {prefix}")
            self.logger.debug(f"Objects: {files}")

            # Sort by the YYYYMMDD portion of filename
            if sort_func:
                latest_file = sorted(
                    files,
                    key=sort_func,
                    reverse=True
                )[0]
            else:
                latest_file = sorted(files, reverse=True)[0]

            self.logger.debug(f"Latest springtail file: {latest_file}")

            # download the file
            filename = os.path.join(local_path, os.path.basename(latest_file))
            self.s3.download_file(bucket, latest_file, filename)

            return filename

        except ClientError as e:
            error_code = e.response['Error']['Code']
            self.logger.error(f"Failed to get latest springtail file: {error_code}")
            print(f"Failed to get latest springtail file: {error_code}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to get latest springtail file: {str(e)}")
            print(f"Failed todd get latest springtail file: {str(e)}")
            return None


    def send_sns_notification(
        self,
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
        try:
            if attributes:
                message_attributes = {
                    k: {'DataType': 'String', 'StringValue': str(v)}
                    for k, v in attributes.items()
                }
            else:
                message_attributes = {}

            self.sns.publish(TopicArn=topic_arn, Message=message, Subject=subject, MessageAttributes=message_attributes)

            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            self.logger.error(f"Failed to send sns message: {error_code}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to send sns message: {str(e)}")
            return False


    def get_secret(self, secret_name: str) -> Optional[list[Dict[str, Any]]]:
        """
        Retrieve a secret from AWS Secrets Manager.
        Returns an optional array of dictionaries.
        """
        # Create a Secrets Manager client
        client = boto3.client("secretsmanager")

        try:
            self.logger.debug(f"Retrieving secret: {secret_name}")

            # Fetch the secret value
            response = client.get_secret_value(SecretId=secret_name)

            # Check if the secret is stored as plaintext or JSON
            if "SecretString" in response:
                secret = response["SecretString"]
            else:
                secret = response["SecretBinary"]

            # Parse the secret and ensure it's a list
            secret_data = json.loads(secret) if isinstance(secret, str) else secret
            if isinstance(secret_data, list):
                return secret_data
            elif isinstance(secret_data, dict):
                # Convert single dict to a list with one element
                return [secret_data]
            else:
                # Handle unexpected formats
                return []

        except Exception as e:
            self.logger.error(f"Error retrieving secret: {e}")
            return None