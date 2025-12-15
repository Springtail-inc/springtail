# Transit VPC provides only 1 isolated subnet that is launched in the same AZ
# as our Springtail resource. This subnet is used as an intermediate layer to
# route traffic between the customer VPCs and workload VPC.
data "aws_region" "current" {}

locals {
  region = data.aws_region.current.region
  # az_id -> subnet_cidr map
  az_subnet_map = {
    for az_id in var.az_ids :
    az_id => cidrsubnet(var.cidr_block, 4, index(var.az_ids, az_id))
  }
  application_vpc_id_peer_map = {
    for peer in var.application_vpc_peerings :
    peer.vpc_id => peer
  }
  // Flattened list of objects: {vpc_id, cidr_block}
  // e.g.,
  // {
  //    vpc_id = "vpc-1234"
  //    cidr_block = "10.1.1.0/24"
  // }
  application_vpc_id_cidr_pairs = toset(flatten([
    for k, v in local.application_vpc_id_peer_map : [
      for cidr in v.cidr_blocks : {
        cidr_block = cidr
        vpc_id     = k
      }
    ]
  ]))
  all_cidr_block_set = toset(
    concat(var.database_vpc_peering.cidr_blocks, flatten([
      for peer in var.application_vpc_peerings : peer.cidr_blocks
    ]))
  )
}
resource "aws_vpc" "transit_vpc" {
  cidr_block           = var.cidr_block
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = {
    Name = "${var.resource_name_prefix}-vpc"
  }
}
resource "aws_subnet" "transit_vpc_subnets" {
  for_each                = local.az_subnet_map
  vpc_id = aws_vpc.transit_vpc.id
  # 32 subnets max per VPC should be enough for most cases
  cidr_block              = each.value
  availability_zone_id    = each.key
  map_public_ip_on_launch = false
  tags = {
    Name = "${var.resource_name_prefix}-subnet-${each.key}"
  }
}
resource "aws_route_table" "transit_vpc_subnets_table" {
  vpc_id = aws_vpc.transit_vpc.id
}
resource "aws_route_table_association" "database_vpc_subnet_associations" {
  for_each       = local.az_subnet_map
  subnet_id      = aws_subnet.transit_vpc_subnets[each.key].id
  route_table_id = aws_route_table.transit_vpc_subnets_table.id
}

# Database VPC Peering
resource "aws_vpc_peering_connection" "database_vpc_peer" {
  vpc_id        = aws_vpc.transit_vpc.id
  peer_vpc_id   = var.database_vpc_peering.vpc_id
  peer_owner_id = var.database_vpc_peering.aws_account_id
  peer_region   = var.database_vpc_peering.region

  tags = {
    Name = "${var.resource_name_prefix}-database-vpc-peering"
  }

  // SPR-738: fix: later we will be updating the peering connection options via API
  lifecycle {
    ignore_changes = [
      requester[0].allow_remote_vpc_dns_resolution,
    ]
  }
}
# Application VPC Peerings
resource "aws_vpc_peering_connection" "application_vpc_peers" {
  for_each      = local.application_vpc_id_peer_map
  vpc_id        = aws_vpc.transit_vpc.id
  peer_vpc_id   = each.value.vpc_id
  peer_owner_id = each.value.aws_account_id
  peer_region   = each.value.region

  tags = {
    Name = "${var.resource_name_prefix}-app-vpc-peering-${each.key}"
  }
  // SPR-738: fix: later we will be updating the peering connection options via API
  lifecycle {
    ignore_changes = [
      requester[0].allow_remote_vpc_dns_resolution,
    ]
  }
}

# Route all traffic to the customer database VPCs through the VPC peer
#
# We need the customer side to:
# 1. Accept the peering connection
# 2. Add routes to the transit VPC CIDR block through the peering connection
resource "aws_route" "db_vpc_peer_routes" {
  for_each = toset(var.database_vpc_peering.cidr_blocks)
  route_table_id            = aws_route_table.transit_vpc_subnets_table.id
  destination_cidr_block    = each.value
  vpc_peering_connection_id = aws_vpc_peering_connection.database_vpc_peer.id
}
resource "aws_route" "app_vpc_peer_routes" {
  for_each                  = local.application_vpc_id_cidr_pairs
  route_table_id            = aws_route_table.transit_vpc_subnets_table.id
  destination_cidr_block    = each.value.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.application_vpc_peers[each.value.vpc_id].id
}

# This default security group allows traffic to and from the
# customer databse VPC, customer application VPCs and this own Transit VPC.
resource "aws_security_group" "transit_vpc_default_sg" {
  name   = "${var.resource_name_prefix}-default-sg"
  vpc_id = aws_vpc.transit_vpc.id
  tags = {
    Name = "${var.resource_name_prefix}-default-sg"
  }
}
resource "aws_vpc_security_group_ingress_rule" "transit_vpc_default_sg_ingress_all_rule" {
  for_each          = local.all_cidr_block_set
  security_group_id = aws_security_group.transit_vpc_default_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = each.value
}
resource "aws_vpc_security_group_ingress_rule" "transit_vpc_default_sg_ingress_self_rule" {
  security_group_id = aws_security_group.transit_vpc_default_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = var.cidr_block
}
resource "aws_vpc_security_group_egress_rule" "transit_vpc_default_sg_egress_all_rule" {
  for_each          = local.all_cidr_block_set
  security_group_id = aws_security_group.transit_vpc_default_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = each.value
}
resource "aws_vpc_security_group_egress_rule" "transit_vpc_default_sg_egress_self_rule" {
  security_group_id = aws_security_group.transit_vpc_default_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = var.cidr_block
}

# -- Begin: VPC Endpoints for STS, SNS and Elastic Load Balancing v2 (NLB)
# We need to access these services from within the Transit VPC

# The security group for the VPC endpoint only allows traffic from the Transit VPC
# to the VPC endpoints.
resource "aws_security_group" "vpce_sg" {
  name   = "${var.resource_name_prefix}-vpce-sg"
  vpc_id = aws_vpc.transit_vpc.id
  tags = {
    Name = "${var.resource_name_prefix}-vpce-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "vpce_sg_ingress_rule" {
  security_group_id = aws_security_group.vpce_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = var.cidr_block
}
resource "aws_vpc_security_group_egress_rule" "vpce_sg_egress_rule" {
  security_group_id = aws_security_group.vpce_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}
resource "aws_vpc_endpoint" "sts_endpoint" {
  vpc_id            = aws_vpc.transit_vpc.id
  service_name      = "com.amazonaws.${local.region}.sts"
  vpc_endpoint_type = "Interface"
  subnet_ids        = [for s in aws_subnet.transit_vpc_subnets : s.id]
  security_group_ids = [aws_security_group.vpce_sg.id]

  private_dns_enabled = true

  tags = {
    Name = "${var.resource_name_prefix}-sts-vpce"
  }
}

resource "aws_vpc_endpoint" "sns_endpoint" {
  vpc_id            = aws_vpc.transit_vpc.id
  service_name      = "com.amazonaws.${local.region}.sns"
  vpc_endpoint_type = "Interface"
  subnet_ids        = [for s in aws_subnet.transit_vpc_subnets : s.id]
  security_group_ids = [aws_security_group.vpce_sg.id]

  private_dns_enabled = true

  tags = {
    Name = "${var.resource_name_prefix}-sns-vpce"
  }
}

resource "aws_vpc_endpoint" "elb_endpoint" {
  vpc_id            = aws_vpc.transit_vpc.id
  service_name      = "com.amazonaws.${local.region}.elasticloadbalancing"
  vpc_endpoint_type = "Interface"
  subnet_ids        = [for s in aws_subnet.transit_vpc_subnets : s.id]
  security_group_ids = [aws_security_group.vpce_sg.id]

  private_dns_enabled = true

  tags = {
    Name = "${var.resource_name_prefix}-elb-vpce"
  }
}
# -- End: VPC Endpoints for STS, SNS and Elastic Load Balancing v2 (NLB)

output "vpc_id" {
  value = aws_vpc.transit_vpc.id
}
# This should be identical to the var.cidr_block
output "vpc_cidr_block" {
  value = aws_vpc.transit_vpc.cidr_block
}
output "vpc_subnet_ids" {
  value = [for k, v in aws_subnet.transit_vpc_subnets : v.id]
}
output "vpc_security_group_id" {
  value = aws_security_group.transit_vpc_default_sg.id
}
output "database_vpc_peering_connection_id" {
  value = aws_vpc_peering_connection.database_vpc_peer.id
}
output "application_vpc_peering_connection_ids" {
  value = {for k, v in aws_vpc_peering_connection.application_vpc_peers : k => v.id}
}
output "customer_cidr_blocks" {
  value = [for k, v in local.all_cidr_block_set : v]
}
output "subnet_cidr_blocks" {
  value = {for k in aws_subnet.transit_vpc_subnets : k.id => k.cidr_block}
}
# -- This is for reference purpose. Output *all* resource ARNs for this Module.
output "all_resource_ids" {
  value = {
    route_table_arn                  = aws_route_table.transit_vpc_subnets_table.arn
    subnets_arns                     = {for k, v in aws_subnet.transit_vpc_subnets : k => v.arn}
    vpc_arn                          = aws_vpc.transit_vpc.arn
    default_security_group_arn       = aws_security_group.transit_vpc_default_sg.arn
    aws_endpoints_security_group_arn = aws_security_group.vpce_sg.arn
    aws_elb_endpoint_arn             = aws_vpc_endpoint.elb_endpoint.arn
    aws_sns_endpoint_arn             = aws_vpc_endpoint.sns_endpoint.arn
    aws_sts_endpoint_arn             = aws_vpc_endpoint.sts_endpoint.arn
    application_vpc_peering_conn_ids = {for k, v in aws_vpc_peering_connection.application_vpc_peers : k => v.id}
    database_vpc_peering_conn_id     = aws_vpc_peering_connection.database_vpc_peer.id
  }
}