# -- Begin: General info
# You may specify anything you like, but want to make sure the `region` is the one you intended to launch your instances.
organization_id      = "1000"
account_id           = "2000"
environment          = "example"
database_instance_id = "1"
custom_domain_name   = "dbi-example-private.db.springtail.dev"
region               = "us-east-1"
# -- End: General info

# -- Begin: Shard specific info
# You need to populate the following shard specific info based on the output
# from the `shard` infra deployment.
shard = {
  shard                 = "s1"
  vpc_id                = "vpc-05399b793bd112166"
  az_id                 = "use1-az1"
  vpc_cidr_block        = "10.1.0.0/16"
  vpc_ipv6_cidr_block   = "2600:1f18:5899:9700::/56"
  public_route_table_id = "rtb-097749bcf45583218"

  redis = {
    cluster_endpoint   = "master.rg-us-east-1-s1.fk7gji.use1.cache.amazonaws.com"
    cluster_port       = 6379
    user               = "LS1ogb2s"
    password_key       = "redis/password/us-east-1/s1/2spPI"
    security_group_ids = ["sg-0eed710b0a3f64598"]
  }
  sns_topic_arn = "arn:aws:sns:us-east-1:891377357651:s1-sns-events"
}
# -- End: Shard specific info

# -- Begin: Customer info
# You need to change the following to your test customer primary DB info.
# The information listed here are the ones we launch for testing purpose.
# Make sure the Primary DB is launched in the same region as specified above.
customer_database = {
  aws_account_id  = "127214175377"
  region          = "us-east-1"
  vpc_id          = "vpc-05f03ea9b118b61ab"
  vpc_cidr_blocks = ["10.254.1.0/24"]
  hostname        = "curry.cn84mcs0029d.us-east-1.rds.amazonaws.com"
  port            = 5432
}
customer_apps = [
  # Applications that need to access Springtail services.
  {
    aws_account_id  = "127214175377"
    region          = "us-east-1"
    vpc_id          = "vpc-05f03ea9b118b61ab"
    vpc_cidr_blocks = ["10.254.1.0/24"]
  }
]
# -- End: customer info

# -- Begin: Networking
# Pick a non-overlapping CIDR block for the transit VPC (with the Customer VPC above)
networking = {
  transit_vpc = {
    cidr_block = "10.123.0.0/16"
  }
  # Derive a subnet CIDR block within the `vpc_cidr_block` from above.
  # Make sure it does not overlap with any existing subnets in the shard VPC.
  # You should already see a `workload_vpc_private_subnet_cidr` output from the `shard` infra deployment.
  # Make sure it does not overlap with that either.
  database_instance_subnet = {
    cidr_block = "10.1.5.0/24"
    nat_gw_id  = "nat-0f4339e2cbc79abfc"
  }
}
# -- End: Networking

# -- Begin: Watcher
watcher = {
  schedule_expression = "rate(1 minute)"
  timeout             = 45
}
# -- End: Watcher

# -- Begin: Springtail services
common_env_vars = {
  DEPLOYMENT_ENV = "example"
}

instance_resource_access = {
  sns_topic_publish_arn    = "arn:aws:sns:us-east-1:381492230138:st-events"
  sns_topic_subscribe_arns = ["arn:aws:sns:us-east-1:381492230138:st-events"]
}

# We use a custom image in us-east-1 with Springtail checked out and all build dependencies installed.
# Use a proper AMI image for your region. The onse used here is for us-east-1 only within Springtail AWS account.
proxy_service = {
  instance_keys     = ["default"]
  machine_image     = "ami-0583f16408797a739"
  instance_type     = "c6gn.medium"
  user_data         = <<EOF
#!/bin/bash
echo "Proxy service user data script run."
EOF
  target_port       = 5432
  health_check_port = 5432
  nlb_listener_port = 10000
  env_vars = {
    LOGGING_VOLUME_MOUNT_POINT = "/shadow_logs"
    SHADOW_LOG_PATH            = "/shadow_logs"
    SPRINGTAIL_LOG_PATH        = "/opt/springtail/logs"
  }
}

fdw_service = {
  instance_keys = ["default"]
  machine_image = "ami-0583f16408797a739"
  instance_type = "c6gn.medium"
  user_data     = <<EOF
#!/bin/bash
echo "FDW service user data script run."
EOF
  env_vars = {
    LOGGING_VOLUME_MOUNT_POINT = "/shadow_logs"
    SHADOW_LOG_PATH            = "/shadow_logs"
    SPRINGTAIL_LOG_PATH        = "/opt/springtail/logs"
  }
}

ingestion_service = {
  instance_keys = ["default"]
  machine_image = "ami-0583f16408797a739"
  instance_type = "c6gn.medium"
  user_data     = <<EOF
#!/bin/bash
echo "Ingestion service user data script run."
EOF
  env_vars = {
    LOGGING_VOLUME_MOUNT_POINT = "/shadow_logs"
    SHADOW_LOG_PATH            = "/shadow_logs"
    SPRINGTAIL_LOG_PATH        = "/opt/springtail/logs"
  }
}

zfs = {
  per_volume_size = 8
  iops            = 3000
  throughput      = 125
  slog_size       = 2
}
# -- End: Springtail services
