import json
import logging
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# Get the parent directory of the current script (i.e., the project root directory)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the /shared directory to the Python path
sys.path.append(os.path.join(project_root, 'shared'))

from common import (
    run_command,
    makedir
)

from aws import AwsHelper

from postgres_component import PostgresComponent

from properties import Properties

S3_BIN_FOLDER = 'packages'
S3_DOWNLOAD_PATH = '/tmp/'

SPRINGTAIL_LIB_DIR = 'shared-lib'  # relative to the install path

# NOTE: this should match the environment variables in common/environment.hh
ENV_VARS = [
    'REDIS_USER',
    'REDIS_PASSWORD',
    'REDIS_USER_DATABASE_ID',
    'REDIS_CONFIG_DATABASE_ID',
    'REDIS_PORT',
    'REDIS_HOSTNAME',
    'REDIS_SSL',
    'ORGANIZATION_ID',
    'ACCOUNT_ID',
    'DATABASE_INSTANCE_ID',
    'LUSTRE_DNS_NAME',
    'LUSTRE_MOUNT_NAME',
    'MOUNT_POINT',
    'FDW_ID',
    'LD_LIBRARY_PATH'
]

SNS_ENV_VARS = [
    'ORGANIZATION_ID',
    'ACCOUNT_ID',
    'DATABASE_INSTANCE_ID',
    'SERVICE_NAME',
    'INSTANCE_KEY',
    'HOSTNAME',
    'FDW_ID',
    'AWS_REGION'
]


class Production:
    """
    Production class to install and manage the production environment.
    """

    def __init__(self, install_path: str, postgres_pid_file: Optional[str] = None):
        """Initialize the production environment.

        Args:
            install_path (str): The path where the springtail binaries will be installed
            postgres_pid_file (Optional[str]): Optional path to the postgres pid file. If not provided, will be the default file, which
                            is determined based on the postgres version and user.
        """
        arn = os.environ.get('SNS_TOPIC_ARN')
        if not arn:
            raise ValueError("SNS_TOPIC_ARN environment variable not set")

        self.topic_arn: str = arn
        self.install_path: str = install_path
        self.postgres_pid_file: Optional[str] = postgres_pid_file

        self.logger = logging.getLogger('springtail')
        self.aws = AwsHelper(
            region=os.environ.get('AWS_REGION', 'us-east-1'),
            # Override endpoints for local testing below
            s3_endpoint=os.environ.get('AWS_S3_ENDPOINT', None),
            sns_endpoint=os.environ.get('AWS_SNS_ENDPOINT', None),
            secretsmanager_endpoint=os.environ.get('AWS_SECRETSMANAGER_ENDPOINT', None)
        )

        self.sns_attributes: Dict[str, Any] = self._extract_attributes()

        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('nose').setLevel(logging.CRITICAL)
        logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
        logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    def install_binaries(self, config_gitsha: str) -> None:
        """
        Install the springtail binaries on the local system.
        Current s3 bucket: s3://data-share.springtail.internal/packages/

        We follow two steps for the installation process:

          1. Download the latest package from S3 to a temporary location
          2. Extract the package and check the config hash, if matches, copy over to the installation path.
             Otherwise, clean up and error out.
        """
        global S3_DOWNLOAD_PATH, S3_BIN_FOLDER

        # Download the springtail binaries
        s3_bucket = os.environ.get('S3_BUCKET', "data-share.springtail.internal")
        if not s3_bucket:
            raise ValueError("S3_BUCKET environment variable not set")

        self.logger.info(f"Downloading springtail binaries from {s3_bucket}/{S3_BIN_FOLDER} to {S3_DOWNLOAD_PATH}")

        self.send_sns('download_start')
        prefix = 'springtail-'
        springtail_tgz = self.aws.s3_download(s3_bucket, S3_BIN_FOLDER,
                                              S3_DOWNLOAD_PATH, prefix,
                                              sort_func=lambda x: x.split(prefix)[1].split('.')[0])
        if not springtail_tgz:
            self.send_sns('download_failed')
            raise ValueError("Failed to download springtail binaries")

        # Extract the springtail_tgz to a temporary location (under S3_DOWNLOAD_PATH) and check the config hash
        temp_dir = tempfile.mkdtemp(dir=S3_DOWNLOAD_PATH)
        try:
            run_command('tar', ['xzf', springtail_tgz, '-C', temp_dir])
        except Exception as e:
            self.logger.error("Failed to extract springtail binaries: %s", str(e))
            self.send_sns('download_failed')
            os.unlink(springtail_tgz)
            shutil.rmtree(temp_dir)
            raise e

        package_config_sha = self.get_config_hash(os.path.join(temp_dir, 'INFO.txt'))
        if package_config_sha != config_gitsha:
            msg = f"Config Git SHA mismatch: expected {config_gitsha}, got {package_config_sha}"
            self.logger.error(msg)
            self.send_sns('config_git_sha_mismatch')
            os.unlink(springtail_tgz)
            shutil.rmtree(temp_dir)
            raise ValueError(msg)

        # Only send this if the config hash matches
        self.send_sns('download_complete', version=(os.path.basename(springtail_tgz)))

        try:
            # Create the installation directory if it doesn't exist
            if not os.path.exists(self.install_path):
                makedir(self.install_path)

            # set LD_LIBRARY_PATH
            os.environ['LD_LIBRARY_PATH'] = os.path.join(self.install_path, SPRINGTAIL_LIB_DIR)

            # Install the binaries and shared libraries by moving the temp_dir contents to install_path
            # Important: for rsync to work correctly, temp_dir and self.install_path must end with a '/'
            run_command('sudo', ['rsync', '-a', os.path.join(temp_dir, ''), os.path.join(self.install_path, '')])

            # Make sure shared-lib is readable by all
            run_command('sudo', ['chmod', '-R', '755', os.path.join(self.install_path, SPRINGTAIL_LIB_DIR)])
            self.logger.info(f"Springtail binaries installed to {self.install_path}")
            self.send_sns('install_complete', version=os.path.basename(springtail_tgz))

        except Exception as e:
            self.logger.error(f"Failed to install springtail binaries: {str(e)}")
            self.send_sns('install_failed', version=os.path.basename(springtail_tgz))
            raise e
        finally:
            try:
                os.unlink(springtail_tgz)
                shutil.rmtree(temp_dir)
            except Exception as e:
                self.logger.warning("Failed to clean up temporary files: %s %s (%s)", springtail_tgz, temp_dir, str(e))

    def get_config_hash(self, file_path: str) -> str:
        """Read and extract Config Hash value from the version file.

        Args:
            file_path: Path to the version file

        Returns:
            String containing the config hash value
        """
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    if line.startswith('Config Hash:'):
                        return line.split(':')[1].strip()
        except Exception as e:
            logging.error(f"Failed to read config hash: {str(e)}")
            raise

        raise ValueError("Config Hash not found in version file")

    def install_pgfdw(self, props: Properties) -> str:
        """
        Install the postgres libraries on the local system for the FDW.
        Should be done prior to starting the ddl mgr.

        Returns:
            The path to the postmaster.pid file
        """
        self.logger.info("Installing Postgres FDW")

        # Get the share and lib directories
        share_dir = run_command('pg_config', ['--sharedir'])
        lib_dir = run_command('pg_config', ['--pkglibdir'])

        # copy the extension files to the share directory
        self.logger.info(f"Copying extension files to the share directory: {share_dir}")
        sp_sharedir = os.path.join(self.install_path, 'share')
        share_dir = os.path.join(share_dir.strip(), 'extension')

        run_command('sudo', ['cp', str(os.path.join(sp_sharedir, 'springtail_fdw--1.0.sql')), share_dir])
        run_command('sudo', ['cp', str(os.path.join(sp_sharedir, 'springtail_fdw.control')), share_dir])

        # copy the shared library to the lib directory
        self.logger.info(f"Copying shared library to the lib directory: {lib_dir}")
        sp_libdir = os.path.join(self.install_path, 'lib')
        lib_dir = os.path.join(lib_dir.strip(), 'springtail_fdw.so')
        run_command('sudo', ['cp', os.path.join(sp_libdir, 'libspringtail_pg_fdw.so'), lib_dir])

        # Update the postgres configuration file
        # version string is like: 'PostgreSQL 16.4 (Ubuntu 16.4-0ubuntu0.24.04.2)'
        self.logger.info("Updating postgres environment file")
        version_str = run_command('pg_config', ['--version']).strip()
        version = version_str.split(' ')[1].split('.')[0]

        fdw_user = props.get_role(Properties.DB_USER_ROLE_FDW)[0]

        env_file = f'/etc/postgresql/{version}/{fdw_user}/environment'
        hba_file = f'/var/lib/postgresql/{version}/{fdw_user}/pg_hba.conf'
        pid_file = self.postgres_pid_file or  f'/var/lib/postgresql/{version}/{fdw_user}/postmaster.pid'

        # Update the localhost socket connection to use scram-sha-256
        self.logger.info("Setting up pg_hba.conf")
        run_command('sudo', ['sed', '-i',
                             's/^local[[:space:]]\\+all[[:space:]]\\+all[[:space:]]\\+\\(md5\\|peer\\)/local   all   all   scram-sha-256/',
                             hba_file])

        # Write the environment variables to a temporary file
        with tempfile.NamedTemporaryFile(delete=True, mode='w') as temp_file:
            # Write data to the temporary file
            for var in ENV_VARS:
                value = os.environ.get(var)
                if value:
                    value = value.replace("'", "''")
                    temp_file.write(f"{var} = '{value}'\n")
            temp_file.flush()

            # Copy the contents of the temporary file to the environment file
            run_command('sudo', ['cp', temp_file.name, env_file])

        # stop postgres
        bindir = run_command('pg_config', ['--bindir']).strip()
        pg = PostgresComponent(name="postgres",
                               id="10",
                               path=bindir,
                               pid_path=pid_file,
                               props=props)
        pg.shutdown()

        return pid_file

    def _extract_attributes(self) -> Dict[str, Any]:
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
        instance_id = self.aws.get_instance_id()
        attributes['AWS_INSTANCE_ID'] = instance_id

        # generate SRN: format: srn:1:1:aws:dbi/82
        srn = f"srn:{attributes['ORGANIZATION_ID']}:{attributes['ACCOUNT_ID']}:aws:dbi/{attributes['DATABASE_INSTANCE_ID']}"
        attributes['SRN'] = srn

        return {k.lower(): v for k, v in attributes.items()}

    def send_sns(
            self,
            type: str,
            component: str = "",
            version: str = "",
            attrs: dict = {}
    ) -> None:
        """
        Send a message to the SNS topic.
        """

        srn = self.sns_attributes['srn']
        service_name = self.sns_attributes['service_name']

        now = datetime.now(timezone.utc)
        timestamp_ms = int(now.timestamp() * 1000)
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        # copy the attributes and add the timestamp
        attributes = dict(self.sns_attributes)
        attributes['epoch_ms'] = timestamp_ms
        attributes['timestamp'] = timestamp
        attributes['source'] = 'coordinator'

        for k, v in attrs.items():
            attributes[k] = v

        msg = ""

        if type == 'startup':
            subject = f"Instance startup: {srn}, {service_name} @{timestamp}"
        elif type == 'shutdown':
            subject = f"Instance shutdown: {srn}, {service_name} @{timestamp}"
        elif type == 'warning':
            subject = f"Warning detected: {srn}, {service_name}, {component} @{timestamp}"
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
        elif type == 'coordinator_reload_failed':
            subject = f"New version coordinator reload failed: {srn}, {service_name} @{timestamp}"
        elif type == 'db_state_change':
            subject = f"Database state change: {srn}, {service_name} @{timestamp}"
            msg = f"\nState change: {attrs['old_state']} -> {attrs['new_state']}"
        elif type == 'max_retries_failed':
            subject = f"Maximum restart retries hit: {srn}, {service_name} @{timestamp}"
            msg = f"Component tried restarting, but couldn't restart after maximum retries: {component}"
            attributes['component'] = component
        elif type == 'config_git_sha_mismatch':
            subject = f"Config Git SHA mismatch: {srn}, {service_name} @{timestamp}"
            msg = "The config git SHA of the downloaded package does not match the expected value in Redis."
        else:
            self.logger.error(f"Unknown SNS message type: {type}")
            return

        message = f"{subject}{msg}\n\n{json.dumps(attributes)}"

        self.logger.info(f"SNS message: {subject}")

        self.aws.send_sns_notification(self.topic_arn, subject, message, attributes)
