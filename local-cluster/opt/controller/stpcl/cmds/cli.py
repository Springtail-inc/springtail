import sys
import os
import click
import docker
import logging
import redis
import boto3

from stpcl.consts import (
    DEPLOYMENT_ENV_DEV,
    DEPLOYMENT_ENV_LOCAL,
    DEPLOYMENT_ENV_PROD,
    DEPLOYMENT_ENV_UNSET,
)

logger = logging.getLogger(__name__)


# Most of the options can be set via environment variables as well, see defaults.
@click.group()
@click.option(
    "--redis-host",
    "-h",
    default=lambda: os.getenv("REDIS_HOST", "localhost"),
    help="Redis host. Default: localhost",
)
@click.option(
    "--redis-port",
    "-p",
    default=lambda: os.getenv("REDIS_PORT", 6379),
    help="Redis port. Default: 6379",
)
@click.option(
    "--redis-user",
    "-u",
    default=lambda: os.getenv("REDIS_USER", None),
    help="Redis user. Default: None",
)
@click.option(
    "--redis-password",
    "-P",
    default=lambda: os.getenv("REDIS_PASSWORD", None),
    help="Redis password. Default: None",
)
@click.option(
    "--redis-db",
    "-d",
    default=lambda: os.getenv("REDIS_DB", 0),
    help="Redis database number. Default: 0",
)
@click.option(
    "--redis-ssl",
    is_flag=True,
    default=lambda: os.getenv("REDIS_SSL", "false").lower() in ("true", "1", "t"),
    help="Use SSL to connect to Redis. Default: False",
)
@click.option(
    "--organization-id",
    "-o",
    default=lambda: os.getenv("ORGANIZATION_ID", 1),
    help="Organization ID to bind to. Default: 1",
)
@click.option(
    "--account-id",
    "-a",
    default=lambda: os.getenv("ACCOUNT_ID", 1),
    help="Account ID to bind to. Default: 1",
)
@click.option(
    "--instance-id",
    "-i",
    default=lambda: os.getenv("DATABASE_INSTANCE_ID", None),
)
@click.option(
    "--deployment-env",
    "-e",
    default=lambda: os.getenv("DEPLOYMENT_ENV", DEPLOYMENT_ENV_UNSET),
)
@click.option(
    "--aws-secretsmanager-endpoint",
    default=lambda: os.getenv("AWS_SECRETSMANAGER_ENDPOINT", None),
    help="Custom endpoint for AWS Secrets Manager (for local testing with LocalStack/Moto etc.)",
)
@click.option(
    "--aws-sns-endpoint",
    default=lambda: os.getenv("AWS_SNS_ENDPOINT", None),
    help="Custom endpoint for AWS SNS (for local testing with LocalStack/Moto etc.)",
)
@click.option(
    "--aws-region",
    default=lambda: os.getenv("AWS_REGION")
                    or os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    help="AWS region for services. Set via AWS_REGION or AWS_DEDAULT_REGION. Default: us-east-1",
)
@click.pass_context
def cli(
        ctx,
        redis_host,
        redis_port,
        redis_user,
        redis_password,
        redis_db,
        redis_ssl,
        organization_id,
        account_id,
        instance_id,
        deployment_env,
        aws_secretsmanager_endpoint,
        aws_sns_endpoint,
        aws_region,
):
    """
    Springtail cluster control CLI. This tool will read the environment variables:
    For Redis connection: REDIS_HOST, REDIS_PORT, REDIS_USER, REDIS_PASSWORD, REDIS_DB
    if not explicitly set via command line arguments. If nothing set, defaults (localhost:6379) will be used.
    """
    from stpcl.imp.utils import DockerCli
    if deployment_env == DEPLOYMENT_ENV_UNSET:
        logger.fatal(
            "DEPLOYMENT_ENV is not set. You need to set it to one of ['%s', '%s', '%s']",
            DEPLOYMENT_ENV_LOCAL,
            DEPLOYMENT_ENV_DEV,
            DEPLOYMENT_ENV_PROD,
        )
        sys.exit(1)

    if not instance_id:
        logger.fatal(
            "DATABASE_INSTANCE_ID is not set. You need to set it to the instance ID you want to manage."
        )
        sys.exit(1)

    if deployment_env == DEPLOYMENT_ENV_LOCAL:
        # Warn if the deployment env is local but the AWS endpoints are not set to local endpoints
        if not (aws_secretsmanager_endpoint and aws_sns_endpoint):
            logger.warning(
                "You are running in 'local' deployment environment but AWS_SECRETSMANAGER_ENDPOINT "
                "and/or AWS_SNS_ENDPOINT are not set. Are you sure?"
            )

    ctx.ensure_object(dict)
    ctx.obj["organization_id"] = int(organization_id)
    ctx.obj["account_id"] = int(account_id)
    ctx.obj["instance_id"] = int(instance_id)
    ctx.obj["redis"] = redis.Redis(
        host=redis_host,
        port=redis_port,
        username=redis_user,
        password=redis_password,
        db=redis_db,
        decode_responses=True,
        ssl=redis_ssl,
    )
    ctx.obj["deployment_env"] = deployment_env
    # inject boto3 clients
    session = boto3.session.Session()

    ctx.obj["aws_secretsmanager_client"] = session.client(
        "secretsmanager",
        region_name=aws_region,
        endpoint_url=aws_secretsmanager_endpoint,
    )
    ctx.obj["aws_sns_client"] = session.client(
        "sns",
        region_name=aws_region,
        endpoint_url=aws_sns_endpoint,
    )
    ctx.obj["docker_cli"] = DockerCli()


@cli.group()
def config():
    """Redis configuration commands."""
    pass


@cli.group()
def notifications():
    """Notifications management commands."""
    pass


@cli.group()
def server():
    """HTTP server commands."""
    pass
