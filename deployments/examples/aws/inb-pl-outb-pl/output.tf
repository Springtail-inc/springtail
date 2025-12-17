output "st_transit_vpc_id" {
  value = module.transit_vpc.vpc_id
}
output "database_vpc_peering_connection_id" {
  value = module.transit_vpc.database_vpc_peering_connection_id
}
# VPC ID -> Peering Connection ID map
# This is for the application VPCs, it can be the same with the primary database VPC.
output "application_vpc_peering_connection_ids" {
  value = merge(
    {
      for k, v in var.customer_apps :
      v.vpc_id => module.transit_vpc.database_vpc_peering_connection_id if v.vpc_id == var.customer_database.vpc_id
    },
    module.transit_vpc.application_vpc_peering_connection_ids
  )
}
output "st_transit_vpc_cidr_block" {
  value = module.transit_vpc.vpc_cidr_block
}
output "st_transit_vpc_subnet_ids" {
  value = module.transit_vpc.vpc_subnet_ids
}
# SPR-738: This SubnetID -> CIDR block map will be stored under the
# `transit_vpc_config` JSONB, which is used by the Springtail API to
# calcualte the CIDR block for the future public subnet CIDR
output "st_transit_vpc_subnet_cidr_blocks" {
  value = module.transit_vpc.subnet_cidr_blocks
}

output "st_inbound_nlb_arn" {
  value = module.inbound_vpc_endpoint_service_nlb.nlb_arn
}
output "st_inbound_nlb_target_group_arn" {
  value = module.inbound_vpc_endpoint_service_nlb.nlb_target_group_arns["inbound"]
}
output "st_inbound_nlb_domain_name" {
  value = module.inbound_vpc_endpoint_service_nlb.nlb_dns_name
}
#
output "st_inbound_nlb_security_group_id" {
  value = "${local.aws_account_id}/${module.inbound_vpc_endpoint_service_nlb.nlb_security_group_id}"
}
output "st_inbound_vpc_endpoint_service_name" {
  value = aws_vpc_endpoint_service.inbound_vpc_endpoint_service.service_name
}
output "st_inbound_vpce_domain_names" {
  value = module.inbound_vpc_endpoint.vpc_endpoint_domain_names
}
# output "st_inbound_watcher_function_arn" {
#   value = module.watcher.watcher_function_arn
# }
output "st_springtail_subnet_id" {
  value = module.springtail_subnet.subnet_id
}
output "st_services_security_group_id" {
  value = module.springtail_subnet.security_group_id
}

output "st_proxy_service_instance_ids" {
  value = module.proxy_service.instance_ids
}
output "st_proxy_service_instance_ips" {
  value = module.proxy_service.instance_ips
}
output "st_ingestion_service_instance_ids" {
  value = module.ingestion_service.instance_ids
}
output "st_ingestion_service_instance_ips" {
  value = module.ingestion_service.instance_ips
}
output "st_fdw_service_instance_ids" {
  value = module.fdw_service.instance_ids
}
output "st_fdw_service_instance_ips" {
  value = module.fdw_service.instance_ips
}


output "st_outbound_nlb_arn" {
  value = module.outbound_vpc_endpoint_service_nlb.nlb_arn
}
output "st_outbound_nlb_domain_name" {
  value = module.outbound_vpc_endpoint_service_nlb.nlb_dns_name
}
output "st_outbound_nlb_target_group_arn" {
  value = module.outbound_vpc_endpoint_service_nlb.nlb_target_group_arns["proxy"]
}
output "st_outbound_nlb_security_group_id" {
  value = module.outbound_vpc_endpoint_service_nlb.nlb_security_group_id
}
output "st_outbound_vpc_endpoint_service_name" {
  value = aws_vpc_endpoint_service.outbound_vpc_endpoint_service.service_name
}
output "st_outbound_vpce_domain_names" {
  value = module.outbound_vpc_endpoint.vpc_endpoint_domain_names
}

output "private_hosted_zone_id" {
  value = module.outbound_vpc_endpoint.hosted_zone_id
}
output "watcher_function_arn" {
  value = module.watcher.watcher_function_arn
}
output "watcher_sns_arn" {
  value = module.watcher.sns_topic_arn
}
