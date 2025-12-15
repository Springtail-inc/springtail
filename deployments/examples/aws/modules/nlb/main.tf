# A thin wrapper of NLB module to create a network load balancer
# with one or more listener and target groups with a few arguments.
# This NLB by default allows egress to anywhere, but restrict ingress
# connections from the given CIDR blocks and on the specific listener port as
# defined in the target_config.

locals {
  # Flattens the Cidr blocks from the target_config
  sg_ingress_config = distinct(flatten([
    for k, v in var.target_config : [
      for c in var.target_config[k].ingress_cidr_blocks : {
        name          = "${k}-${c}"
        listener_port = var.target_config[k].listener_port
        protocol      = var.target_config[k].protocol
        cidr_block    = c
      }
    ]
  ]))
}

resource "aws_security_group" "nlb_sg" {
  name   = "${var.resource_name_prefix}-nlb-sg"
  description = "Security group for the inbound NLB"
  # Ref to the transit_vpc
  vpc_id = var.vpc_id
  tags = {
    Name = "${var.resource_name_prefix}-nlb-sg"
  }
}
# Allowing others from the given CIDR blocks to connect to the NLB.
# For the scenario where the NLB is used to connect to the customer DB instance
# this need to be the CIDR blocks of the customer VPC
resource "aws_vpc_security_group_ingress_rule" "nlb_sg_ingress_rule" {
  for_each          = {for entry in local.sg_ingress_config : entry.name => entry}
  security_group_id = aws_security_group.nlb_sg.id
  from_port         = each.value.listener_port
  to_port           = each.value.listener_port
  ip_protocol = lower(each.value.protocol)
  cidr_ipv4         = each.value.cidr_block
}
resource "aws_vpc_security_group_egress_rule" "nlb_sg_egress_rule" {
  security_group_id = aws_security_group.nlb_sg.id
  ip_protocol = "-1"
  # For simplicity, allow all egress traffic to anywhere for now.
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_lb" "nlb" {
  name                             = "${var.resource_name_prefix}-nlb"
  load_balancer_type               = "network"
  enable_cross_zone_load_balancing = var.enable_cross_zone_load_balancing
  internal                         = var.internal
  security_groups = [aws_security_group.nlb_sg.id]
  subnets                          = var.subnet_ids
}
resource "aws_lb_target_group" "nlb_tg" {
  for_each    = var.target_config
  name        = "${var.resource_name_prefix}-${each.key}-tg"
  port        = each.value.target_port
  protocol    = each.value.protocol
  vpc_id      = var.vpc_id
  target_type = each.value.type
  health_check {
    protocol            = each.value.protocol
    unhealthy_threshold = each.value.unhealthy_threshold
    interval            = each.value.interval
    timeout             = each.value.interval - 1
    port                = each.value.health_check_port != 0 ? each.value.health_check_port : each.value.target_port
  }
  lifecycle {
    create_before_destroy = true
  }
}
resource "aws_lb_listener" "nlb_listener" {
  for_each          = var.target_config
  load_balancer_arn = aws_lb.nlb.arn
  port              = each.value.listener_port
  protocol          = each.value.protocol

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.nlb_tg[each.key].arn
  }
}

output "nlb_arn" {
  value = aws_lb.nlb.arn
}
output "nlb_dns_name" {
  value = aws_lb.nlb.dns_name
}
# Output a map from the target config name to its ARN
output "nlb_target_group_arns" {
  value = {
    for k, v in var.target_config : k => aws_lb_target_group.nlb_tg[k].arn
  }
}
output "nlb_security_group_id" {
  value = aws_security_group.nlb_sg.id
}
output "nlb_zone_id" {
  value = aws_lb.nlb.zone_id
}
# -- This is for reference purpose. Output *all* resource ARNs for this Module.
output "all_resource_ids" {
  value = {
    security_group_arn = aws_security_group.nlb_sg.arn
    listener_arns      = {for k, v in aws_lb_listener.nlb_listener : k => v.arn}
    nlb_arn            = aws_lb.nlb.arn
    target_group_arns  = {for k, v in aws_lb_target_group.nlb_tg : k => v.arn}
  }
}