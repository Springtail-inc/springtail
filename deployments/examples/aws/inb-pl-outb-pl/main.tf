# Refer to this https://app.diagrams.net/#G1Gi8Yi6Lc0QAlVEQh-iPoL77tmhRFj8ID#%7B%22pageId%22%3A%22Ht1M8jgEwFfnCIfOTk4-%22%7D
data "aws_caller_identity" "current" {}
locals {
  aws_account_id       = data.aws_caller_identity.current.account_id
  resource_name_prefix = "dbi-${var.database_instance_id}"
  dbi_region           = var.region
  dbi_shard            = var.shard.shard
  az_id                = var.shard.az_id
  shard_vpc_id         = var.shard.vpc_id
  # Split and reassemble the custom domain name to get the base and sub domain names.
  # The custom domain name need to be least 3 parts, e.g., 'a.b.c'.
  # Then the following will split it into ['a', 'b.c'], basically, the left most part as the subdomain name
  # then the reset as the base domain name. Then we will be creating a Private Hosted Zone for the
  # base domain; then add a CNAME with the full domain name to as the outbound VPC endpoint DNS name.
  domain_parts            = split(".", var.custom_domain_name)
  domain_length           = length(local.domain_parts)
  custom_sub_domain_name  = join(".", slice(local.domain_parts, 0, 1))
  custom_base_domain_name = join(".", slice(local.domain_parts, 1, local.domain_length))
}


# -- Begin: Transit VPC
# The VPC that acts as a transit between the customer VPC and the workload VPC.
# It will peer with the customer VPC and host NLB that will be the surrogate to the customer primary database.
module "transit_vpc" {
  source               = "../modules/transit-vpc"
  resource_name_prefix = "${local.resource_name_prefix}-transit"
  cidr_block           = var.networking.transit_vpc.cidr_block
  az_ids               = [local.az_id]
  # For peering with the database VPC, which we need to connect into
  database_vpc_peering = {
    # The VPC ID of the customer database VPC
    aws_account_id = var.customer_database.aws_account_id
    region         = var.customer_database.region
    vpc_id         = var.customer_database.vpc_id
    cidr_blocks    = var.customer_database.vpc_cidr_blocks
  }
  # For peering with the application VPCs where customers host their app to access Springtail services.
  application_vpc_peerings = [
    for vpc_info in var.customer_apps : {
      aws_account_id = vpc_info.aws_account_id
      region         = vpc_info.region
      vpc_id         = vpc_info.vpc_id
      cidr_blocks    = vpc_info.vpc_cidr_blocks
    } if vpc_info.vpc_id != var.customer_database.vpc_id
  ]
}
# -- End: Transit VPC

# -- Begin: Database Instance specific subnet (Workload VPC)
# The subnet dedicated to this database instance that hosts all the Springtail services.
module "springtail_subnet" {
  source               = "../modules/st-svc-subnet"
  resource_name_prefix = "${local.resource_name_prefix}-subnet"
  vpc_id               = var.shard.vpc_id
  subnet_cidr_block    = var.networking.database_instance_subnet.cidr_block
  az_id                = local.az_id
  allowed_sg_ids = concat(
    var.shard.redis.security_group_ids,
  )
  nat_gw_id = var.networking.database_instance_subnet.nat_gw_id
}
# -- End: Database Instance specific subnet (Workload VPC)

# -- Begin: Inbound Path
# The NLB inside the Transit VPC as the surrogate to the customer primary database inside the Transit VPC.
module "inbound_vpc_endpoint_service_nlb" {
  source               = "../modules/nlb"
  resource_name_prefix = "${local.resource_name_prefix}-inbound"
  vpc_id               = module.transit_vpc.vpc_id
  subnet_ids           = module.transit_vpc.vpc_subnet_ids
  target_config = {
    # Output target groups are keyed by this name too
    "inbound" = {
      listener_port       = var.customer_database.port
      target_port         = var.customer_database.port
      protocol            = "TCP"
      unhealthy_threshold = 2
      interval            = 5
      # Note: This is important from security perspective.
      # It only allows the resources inside this database instance specific subnet to access the NLB through the VPC
      # endpoint service.
      ingress_cidr_blocks = [var.networking.database_instance_subnet.cidr_block]
      type                = "ip"
    }
  }
}
# The VPC endpoint service backed by the inbound NLB. It pairs with a VPC endpoint inside the Workload VPC to
# establish a PrivateLink that enables our ingestion services to access the customer primary database.
resource "aws_vpc_endpoint_service" "inbound_vpc_endpoint_service" {
  acceptance_required        = false
  network_load_balancer_arns = [module.inbound_vpc_endpoint_service_nlb.nlb_arn]
  tags = {
    Name = "${local.resource_name_prefix}-inbound-vpc-endpoint-service"
  }
}
# The VPC endpoint that pairs with the VPC endpoint service above that is launched inside the Workload VPC.
# The Springtail services use this VPC endpoint's domain name to access the customer primary database.
module "inbound_vpc_endpoint" {
  source                    = "../modules/vpce"
  vpc_id                    = local.shard_vpc_id
  vpc_endpoint_service_name = aws_vpc_endpoint_service.inbound_vpc_endpoint_service.service_name
  inbound_cidr_blocks       = [var.networking.database_instance_subnet.cidr_block]
  subnet_ids                = [module.springtail_subnet.subnet_id]
  name                      = "${local.resource_name_prefix}-inbound-vpc-endpoint"
}
# -- End: Inbound Path

# -- Begin: Outbound Path
# The NLB launched inside the database instance specific subnet of the Workload VPC. This NLB is placed in front of the
# proxy ECS service and serves as the surrogate for our customers to access the Proxy service.
module "outbound_vpc_endpoint_service_nlb" {
  source               = "../modules/nlb"
  resource_name_prefix = "${local.resource_name_prefix}-outbound"
  vpc_id               = local.shard_vpc_id
  subnet_ids           = [module.springtail_subnet.subnet_id]
  target_config = {
    # Output target groups are keyed by this name too
    "proxy" = {
      listener_port       = var.proxy_service.nlb_listener_port
      target_port         = var.proxy_service.target_port
      health_check_port   = var.proxy_service.health_check_port
      protocol            = "TCP"
      unhealthy_threshold = 2
      interval            = 10
      ingress_cidr_blocks = concat(
        [module.transit_vpc.vpc_cidr_block],
        module.transit_vpc.customer_cidr_blocks
      )
      type = "instance"
    }
    # We add another "dummy" group that exposes the health check port directly.
    # This is useful if we have any downstream NLB that points to this NLB, we
    # then have this specific Health-check only target group to point to.
    "dummy" = {
      listener_port       = var.proxy_service.health_check_port
      target_port         = var.proxy_service.health_check_port
      health_check_port   = var.proxy_service.health_check_port
      protocol            = "TCP"
      unhealthy_threshold = 2
      interval            = 10
      ingress_cidr_blocks = concat(
        [module.transit_vpc.vpc_cidr_block],
        module.transit_vpc.customer_cidr_blocks
      )
      type = "instance"
    }
  }
}
# The VPC endpoint service backed by the outbound NLB. It pairs with a VPC endpoint inside the Transit VPC to
# establish a PrivateLink that enables the customer applications to access the Springtail services (proxy).
resource "aws_vpc_endpoint_service" "outbound_vpc_endpoint_service" {
  acceptance_required        = false
  network_load_balancer_arns = [module.outbound_vpc_endpoint_service_nlb.nlb_arn]
  tags = {
    Name = "${local.resource_name_prefix}-outbound-vpc-endpoint-service"
  }
}

# The VPC endpoint that pairs with the VPC endpoint service above that is launched inside the Transit VPC.
# The customer applications use this VPC endpoint's domain name to access Springtail services (proxy).
# Custom domain name format:
# '<hostname>.<shard>.<region>.<environment>.springtail.dev' or non-production;
# '<hostname>.<shard>.<region>.springtail.io' for production;
module "outbound_vpc_endpoint" {
  depends_on = [
    aws_vpc_endpoint_service.outbound_vpc_endpoint_service
  ]
  source                    = "../modules/vpce"
  vpc_id                    = module.transit_vpc.vpc_id
  vpc_endpoint_service_name = aws_vpc_endpoint_service.outbound_vpc_endpoint_service.service_name
  # Allow all customer peered VPC CIDRs to access this VPC endpoint
  # SPR-738: Allow the Transit VPC CIDR block to access the outbound VPC endpoint. For the public endpoint feature:
  # the internet facing NLB launched inside a public subnet of the Transit VPC needs to access this VPC endpoint.
  inbound_cidr_blocks = concat(
    module.transit_vpc.customer_cidr_blocks,
    [module.transit_vpc.vpc_cidr_block]
  )
  subnet_ids = module.transit_vpc.vpc_subnet_ids
  name       = "${local.resource_name_prefix}-outbound-vpc-endpoint"
  # Note that we are *not* done yet!
  # We need to provide customers some instruction to associate their VPC to the Private Hosted Zone
  # In order for them to resolve the domain name to the VPC endpoint.
  custom_domain = {
    base_domain_name = local.custom_base_domain_name
    sub_domain_name  = local.custom_sub_domain_name
    # Attach the transit VPC as the initial VPC
    initial_vpc = {
      vpc_id     = module.transit_vpc.vpc_id
      vpc_region = local.dbi_region
    }
    resolvable_vpcs = [
      for vpc_info in var.customer_apps : {
        vpc_id     = vpc_info.vpc_id
        vpc_region = vpc_info.region
      }
    ]
  }
}
# -- End: Outbound Path

# -- Begin: The Watcher
module "watcher" {
  source               = "../modules/watcher"
  resource_name_prefix = "${local.resource_name_prefix}-watcher"
  database_instance_id = var.database_instance_id
  vpc_subnet_ids       = module.transit_vpc.vpc_subnet_ids
  vpc_sg_id            = module.transit_vpc.vpc_security_group_id
  hostname_to_check    = var.customer_database.hostname

  target_group_arn    = module.inbound_vpc_endpoint_service_nlb.nlb_target_group_arns["inbound"]
  schedule_expression = var.watcher.schedule_expression
  port                = var.customer_database.port
  timeout             = var.watcher.timeout
  sns_topic_arn       = var.shard.sns_topic_arn
}
# -- End: The Watcher

# -- Begin: Springtail Proxy, FDW, and Ingestion Services
locals {
  # This config is across the board for all EC2 instances.
  service_config = {
    organization_id      = var.organization_id
    account_id           = var.account_id
    database_instance_id = var.database_instance_id
    region               = local.dbi_region
    shard                = var.shard
  }
}
# Gives the correct policy for the EC2 instances.
module "instance_profile" {
  source               = "../modules/instance-profile"
  resource_name_prefix = "${local.resource_name_prefix}-instance-profile"
  sns_topic_publish_arns = concat(
    [module.watcher.sns_topic_arn],
    var.instance_resource_access.sns_topic_publish_arn != null ? [var.instance_resource_access.sns_topic_publish_arn]
    : []
  )
  sns_topic_subscribe_arns = concat(
    [module.watcher.sns_topic_arn],
    var.instance_resource_access.sns_topic_subscribe_arns != null ?
    var.instance_resource_access.sns_topic_subscribe_arns : []
  )
}


module "proxy_service" {
  depends_on = [
    module.springtail_subnet,
    module.ingestion_service,
  ]
  source                      = "../modules/database-instance-service-ec2"
  instance_keys               = var.proxy_service.instance_keys != null ? var.proxy_service.instance_keys : toset([])
  resource_name_prefix        = "${local.resource_name_prefix}-proxy"
  instance_ami_id             = var.proxy_service.machine_image
  instance_type               = var.proxy_service.instance_type
  instance_subnet_id          = module.springtail_subnet.subnet_id
  instance_profile_name       = module.instance_profile.instance_profile_name
  instance_security_group_ids = [module.springtail_subnet.security_group_id]
  user_data                   = var.proxy_service.user_data
  volume_size                 = var.proxy_service.volume_size

  env_vars = merge(
    var.common_env_vars,
    var.proxy_service.env_vars,
    {
      SERVICE_NAME          = "proxy"
      SNS_WATCHER_TOPIC_ARN = module.watcher.sns_topic_arn
      NFS_SERVER_IP         = values(module.ingestion_service.instance_ips)[0]
    }
  )
  service_config = local.service_config

  logging_volume = {
    mount_point = "/shadow_logs"
    device_name = "/dev/sdi"
    size        = 10
    throughput  = 125
    iops        = 3000
  }
}

# Attach Proxy instance to Proxy target group and Dummy target group
resource "aws_lb_target_group_attachment" "tg_attach_proxy_tg" {
  target_group_arn = module.outbound_vpc_endpoint_service_nlb.nlb_target_group_arns["proxy"]
  target_id        = values(module.proxy_service.instance_ids)[0]
  port             = var.proxy_service.target_port
}
resource "aws_lb_target_group_attachment" "tg_attach_dummy_tg" {
  target_group_arn = module.outbound_vpc_endpoint_service_nlb.nlb_target_group_arns["dummy"]
  target_id        = values(module.proxy_service.instance_ids)[0]
  port             = var.proxy_service.health_check_port
}

module "fdw_service" {
  depends_on = [
    module.springtail_subnet,
    module.ingestion_service,
  ]
  source                      = "../modules/database-instance-service-ec2"
  instance_keys               = var.fdw_service.instance_keys != null ? var.fdw_service.instance_keys : toset([])
  resource_name_prefix        = "${local.resource_name_prefix}-fdw"
  instance_ami_id             = var.fdw_service.machine_image
  instance_type               = var.fdw_service.instance_type
  instance_subnet_id          = module.springtail_subnet.subnet_id
  instance_profile_name       = module.instance_profile.instance_profile_name
  instance_security_group_ids = [module.springtail_subnet.security_group_id]
  user_data                   = var.fdw_service.user_data
  volume_size                 = var.fdw_service.volume_size

  env_vars = merge(
    var.common_env_vars,
    var.fdw_service.env_vars,
    {
      SERVICE_NAME          = "fdw"
      SNS_WATCHER_TOPIC_ARN = module.watcher.sns_topic_arn
      NFS_SERVER_IP         = values(module.ingestion_service.instance_ips)[0]
    }
  )
  service_config = local.service_config

}

module "ingestion_service" {
  source               = "../modules/database-instance-service-ec2"
  resource_name_prefix = "${local.resource_name_prefix}-ingestion"
  instance_keys = (
    var.ingestion_service.instance_keys != null ? var.ingestion_service.instance_keys :
    toset([])
  )
  instance_ami_id             = var.ingestion_service.machine_image
  instance_type               = var.ingestion_service.instance_type
  instance_subnet_id          = module.springtail_subnet.subnet_id
  instance_profile_name       = module.instance_profile.instance_profile_name
  instance_security_group_ids = [module.springtail_subnet.security_group_id]
  user_data                   = var.ingestion_service.user_data
  volume_size                 = var.ingestion_service.volume_size

  env_vars = merge(
    var.common_env_vars,
    var.ingestion_service.env_vars,
    {
      SERVICE_NAME          = "ingestion"
      SNS_WATCHER_TOPIC_ARN = module.watcher.sns_topic_arn
    }
  )
  service_config = local.service_config

}

# -- Attach the EBS volumes to Ingestion service instance
# The Ingestion service will serve as the NFS server over the ZFS volumes.
module "ebs_zfs" {
  depends_on = [
    module.ingestion_service,
  ]
  count             = var.zfs != null ? 1 : 0
  source            = "../modules/ebs-zfs"
  instance_id       = values(module.ingestion_service.instance_ids)[0]
  availability_zone = module.ingestion_service.instance_availability_zone
  per_volume_size   = var.zfs.per_volume_size
  iops              = var.zfs.iops
  throughput        = var.zfs.throughput
  slog_size         = var.zfs.slog_size
}

# -- End: Springtail Proxy, FDW, and Ingestion Services

