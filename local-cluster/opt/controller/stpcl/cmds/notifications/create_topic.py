import click
import logging

from ..cli import notifications

logger = logging.Logger("stpcl.notifications.create-topic")


@notifications.command("create-topic")
@click.option(
    "--topic-name",
    type=str,
    required=True,
    help="Name of the notification topic to create.",
)
@click.pass_obj
def create_topic(
        obj,
        topic_name: str,
):
    from stpcl.imp.notifications import create_topic
    sns_client = obj["aws_sns_client"]
    topic_arn = create_topic(sns_client, topic_name)
    logger.info(f"Created topic '{topic_name}' with ARN: {topic_arn}")
    print(topic_arn)
