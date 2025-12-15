# Description: This Lambda function is responsible for registering the IP address of the database instance with the
# Target Group of the Network Load Balancer for inbound access. It also deregisters the old IP addresses if they are
# not healthy anymore. The function is intended to run every 60 seconds, and but each run it will try 3 times.
import socket
import os
import boto3
import typing
import time

elbv2_client = boto3.client('elbv2')
sns_client = boto3.client('sns')

# The timeout for the Lambda is 45 seconds, enough time for us to run three times.
max_wait_time = 14

database_instance_id = os.environ['DATABASE_INSTANCE_ID']


def notify(sns_topic_arn, subject, message):
    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject=subject
    )
    print(f"Notification sent: {response}")


def wait_until_target_is_healthy(target_group_arn, ip_address, timeout):
    c = 0
    while c < timeout:
        try:
            registered_ips = get_registered_targets(target_group_arn, ip_address)
        except Exception as e:
            print(f"Failed to get the health state of {ip_address}: {e}")
            continue
        if ip_address in registered_ips and registered_ips[ip_address] == 'healthy':
            break
        time.sleep(5)
        c += 5
    else:
        print(
            f"Failed to wait until {ip_address} is healthy, maximum wait time reached ({timeout}). "
            f"This does not necessarily mean the registration failed, maybe need longer to become healthy."
        )


def get_registered_targets(target_group_arn, ip_address) -> typing.Mapping[str, str]:
    """
    Returns a map of registered target IPs in the Target Group, mapped to their healty state.
    Health state can be one of the followings,

        'initial'|'healthy'|'unhealthy'|'unhealthy.draining'|'unused'|'draining'|'unavailable'
    """
    response = elbv2_client.describe_target_health(
        TargetGroupArn=target_group_arn,
        Targets=[
            {
                'Id': ip_address,
            },
        ]
    )
    return {ent['Target']['Id']: ent['TargetHealth']['State'] for ent in response['TargetHealthDescriptions']}


def run(timeout) -> int:
    """
    Registers the IP address of the database instance with the Target Group. And wait for `timeout` seconds to
    ensure the IP address is healthy.

    :param timeout:
    :return: Run time of the function in seconds.
    """
    start = time.time()
    hostname = os.environ['HOSTNAME_TO_CHECK']
    target_group_arn = os.environ['TARGET_GROUP_ARN']
    port = os.environ['PORT']
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    cross_vpc = os.environ.get('CROSS_VPC', 'false').lower() == 'true'

    ip_address = socket.gethostbyname(hostname)
    print(f"Resolved hostname: {hostname} => {ip_address}")
    # Registers this IP address with a Target Group, if it has not been registered yet
    # or the IP address has changed.
    registered_ips = get_registered_targets(target_group_arn, ip_address)
    if ip_address not in registered_ips or registered_ips[ip_address] in ['unused']:
        # Register!
        print(f"Registering {ip_address} with the Target Group.")
        try:
            target_kwargs = {
                'Id': ip_address,
                'Port': int(port),
            }
            if cross_vpc:
                target_kwargs['AvailabilityZone'] = 'all'

            resp = elbv2_client.register_targets(
                TargetGroupArn=target_group_arn,
                Targets=[target_kwargs]
            )

            notify(sns_topic_arn, f"ip_target_updated|{database_instance_id}|{ip_address}",
                   f"target_group_arn|{target_group_arn}")
            wait_until_target_is_healthy(target_group_arn, ip_address, timeout)
        except Exception as e:
            print(f"Failed to register {ip_address} with the Target Group: {str(e)}")
            return int(time.time() - start)
    else:
        # IP registered, if the health state is not healthy,
        # it is ok, this is intended to run on the next iteration.
        state = registered_ips[ip_address]
        if state == 'healthy':
            # Deregister old IP addresses
            for ip, state in registered_ips.items():
                if ip != ip_address:
                    print(f"Deregistering {ip} from the Target Group.")
                    try:
                        elbv2_client.deregister_targets(
                            TargetGroupArn=target_group_arn,
                            Targets=[
                                {
                                    'Id': ip,
                                },
                            ]
                        )
                        notify(sns_topic_arn, f"ip_target_deregistered|{database_instance_id}|{ip}",
                               f"target_group_arn|{target_group_arn}")
                    except Exception as e:
                        print(f"Failed to deregister {ip} from the Target Group: {str(e)}")
        else:
            if state not in ['initial', 'unused', 'draining']:
                notify(sns_topic_arn, f"ip_target_not_healthy|{database_instance_id}|{ip_address}",
                       f"target_group_arn|{target_group_arn}|{state}")

    return int(time.time() - start)


def lambda_handler(event, context):
    for _ in range(3):
        runtime = run(max_wait_time)
        delta = max_wait_time - runtime
        if delta > 0:
            time.sleep(delta)
