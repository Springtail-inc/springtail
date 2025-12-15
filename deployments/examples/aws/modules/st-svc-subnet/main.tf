# Springtail services, launched in side the workload VPC
# -- Firstly a dedicate ISOLATED subnet for the Springtail services
locals {
  allowed_sg_id_set = toset(var.allowed_sg_ids)
}
resource "aws_subnet" "springtail_subnet" {
  vpc_id = var.vpc_id
  # This has something to do with the Workload VPC's CIDR block
  cidr_block           = var.subnet_cidr_block
  availability_zone_id = var.az_id
  tags = {
    Name = "${var.resource_name_prefix}-subnet"
  }
}
# Simplifies the access between internal services, basically
# Allows all direction traffic within the Springtail subnet
resource "aws_security_group" "springtail_default_sg" {
  name        = "${var.resource_name_prefix}-springtail-sg"
  description = "Security group for the Springtail services. It has two default rules to allow all traffic between components inside the Springtail subnet"
  vpc_id      = var.vpc_id
  tags = {
    Name = "${var.resource_name_prefix}-subnet-sg"
  }
}
resource "aws_vpc_security_group_ingress_rule" "springtail_sg_ingress_all_rule" {
  security_group_id = aws_security_group.springtail_default_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = var.subnet_cidr_block
}
resource "aws_vpc_security_group_egress_rule" "springtail_sg_egress_all_rule" {
  security_group_id = aws_security_group.springtail_default_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = var.subnet_cidr_block
}
# Allow traffic between the Springtail services and all resources tagged with the given security groups
resource "aws_vpc_security_group_ingress_rule" "allowed_ingress_rule" {
  for_each                     = local.allowed_sg_id_set
  security_group_id            = aws_security_group.springtail_default_sg.id
  description                  = "Allow ingress traffic from resources with Security Group IDs."
  ip_protocol                  = "-1"
  referenced_security_group_id = each.value
}
resource "aws_vpc_security_group_egress_rule" "allowed_egress_rule_subnet_internal" {
  for_each                     = local.allowed_sg_id_set
  security_group_id            = aws_security_group.springtail_default_sg.id
  description                  = "Allow egress traffic to resources with Security Group IDs."
  ip_protocol                  = "-1"
  referenced_security_group_id = each.value
}

resource "aws_vpc_security_group_egress_rule" "allowed_egress_rule_outside_1" {
  security_group_id = aws_security_group.springtail_default_sg.id
  description       = "Allow egress traffic to 0.0.0.0/0 on port 80."
  ip_protocol       = "tcp"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  to_port           = 80
}
resource "aws_vpc_security_group_egress_rule" "allowed_egress_rule_outside_2" {
  security_group_id = aws_security_group.springtail_default_sg.id
  description       = "Allow egress traffic to 0.0.0.0/0 on port 443."
  ip_protocol       = "tcp"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
}

# Default route table for the Subnet a private Subnet with 1 route to the Transit Gateway
resource "aws_route_table" "springtail_route_table" {
  vpc_id = var.vpc_id
  tags = {
    Name = "${var.resource_name_prefix}-subnet-rt"
  }
}
resource "aws_route" "springtail_route" {
  route_table_id         = aws_route_table.springtail_route_table.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = var.nat_gw_id
}
resource "aws_route_table_association" "springtail_route_table_association" {
  subnet_id      = aws_subnet.springtail_subnet.id
  route_table_id = aws_route_table.springtail_route_table.id
}

# - Output
# This security group can be used by all the resources launched in this subnet
# to allow traffic from the Springtail services to other resources (identified by their security groups)
output "subnet_id" {
  value = aws_subnet.springtail_subnet.id
}
output "security_group_id" {
  value = aws_security_group.springtail_default_sg.id
}
# -- This is for reference purpose. Output *all* resource ARNs for this Module.
output "all_resource_ids" {
  value = {
    default_security_group_arn = aws_security_group.springtail_default_sg.arn
    subnet_arn                 = aws_subnet.springtail_subnet.arn
    route_table_arn            = aws_route_table.springtail_route_table.arn
  }
}
