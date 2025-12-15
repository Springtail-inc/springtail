# Thin wrapper for quickly create a VPC endpoint to consume a given VPC endpoint service
# with optional Private Domain Name setup.
data aws_region current {}

locals {

}

resource "aws_security_group" "vpce_sg" {
  name   = "${var.name}-vpce-sg"
  vpc_id = var.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "vpce_sg_ingress_rule" {
  for_each = toset(var.inbound_cidr_blocks)
  security_group_id = aws_security_group.vpce_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = each.value
}

resource "aws_vpc_security_group_egress_rule" "vpce_sg_egress_rule" {
  security_group_id = aws_security_group.vpce_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_endpoint" "vpce" {
  vpc_id            = var.vpc_id
  service_name      = var.vpc_endpoint_service_name
  vpc_endpoint_type = "Interface"
  security_group_ids = [aws_security_group.vpce_sg.id]
  subnet_ids        = var.subnet_ids

  tags = {
    Name = var.name
  }
}

# Create a dedicated private hosted zone for the VPC endpoint
# if the custom_domain_name is provided.
resource "aws_route53_zone" "vpce_private_zone" {
  for_each = var.custom_domain != null ? toset([var.custom_domain.base_domain_name]) : toset([])
  name     = var.custom_domain.base_domain_name
  vpc {
    vpc_id     = var.custom_domain.initial_vpc.vpc_id
    vpc_region = var.custom_domain.initial_vpc.vpc_region
  }
  lifecycle {
    ignore_changes = [
      vpc,
    ]
  }
}

# Use CNAME to associate the VPC endpoint DNS name to the custom domain name
# The key here is: CNAME record cannot be the same as the base domain. That is why
# we need  splitting the fed in custom_domain_name into a bas_domain_name and a sub-domain for such purpose.
resource "aws_route53_record" "vpce_dns_record" {
  depends_on = [
    aws_route53_zone.vpce_private_zone,
  ]
  for_each = var.custom_domain != null ? {
    for v in var.custom_domain.resolvable_vpcs : "${v.vpc_region}-${v.vpc_id}" => v
  } : {}
  zone_id = aws_route53_zone.vpce_private_zone[var.custom_domain.base_domain_name].id
  name    = "${var.custom_domain.sub_domain_name}.${var.custom_domain.base_domain_name}"
  type    = "CNAME"
  ttl     = 300
  records = [aws_vpc_endpoint.vpce.dns_entry[0]["dns_name"]]
}

resource "aws_route53_vpc_association_authorization" "vpce_dns_vpc_association_auth" {
  for_each = var.custom_domain != null ? {
    for v in var.custom_domain.resolvable_vpcs : "${v.vpc_region}-${v.vpc_id}" => v
  } : {}
  vpc_id     = each.value.vpc_id
  vpc_region = each.value.vpc_region
  zone_id    = aws_route53_zone.vpce_private_zone[var.custom_domain.base_domain_name].id
}

output "vpc_endpoint_id" {
  value = aws_vpc_endpoint.vpce.id
}

output "vpc_endpoint_domain_names" {
  value = [for n in aws_vpc_endpoint.vpce.dns_entry : n.dns_name]
}

output "vpc_custom_domain_name" {
  value = var.custom_domain != null ? "${var.custom_domain.sub_domain_name}.${var.custom_domain.base_domain_name}" : null
}

output "hosted_zone_id" {
  value = try(aws_route53_zone.vpce_private_zone[var.custom_domain.base_domain_name].id, null)
}

# -- This is for reference purpose. Output *all* resource ARNs for this Module.
output "all_resource_ids" {
  value = {
    endpoint_arn                = aws_vpc_endpoint.vpce.arn
    endpoint_security_group_arn = aws_security_group.vpce_sg.arn
    hosted_zone_id = try(aws_route53_zone.vpce_private_zone[var.custom_domain.base_domain_name].id, null)
    hosted_zone_dns_record_aliases = try({for k, v in aws_route53_record.vpce_dns_record : k => v.alias}, null)
    hosted_zone_vpc_association_auth_ids = try({
      for k, v in aws_route53_vpc_association_authorization.vpce_dns_vpc_association_auth : k => v.id
    }, null)
  }
}