# Description: AWS SNS topic creation and HTTP subscription helper functions.

def create_topic(sns_client, topic_name):
    response = sns_client.create_topic(Name=topic_name)
    return response['TopicArn']


def subscribe_to_topic_http(sns_client, topic_arn, endpoint):
    response = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol='http',
        Endpoint=endpoint,
        ReturnSubscriptionArn=True
    )
    return response['SubscriptionArn']
