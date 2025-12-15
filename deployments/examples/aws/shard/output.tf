output "aws_account_id" {
  value = data.aws_caller_identity.current.account_id
}

# We need to AZ so that we will try launching all our resources into the same AZ to
# get the best performance initially. Failover will be handled later.
output "az_id" {
  value = var.az_id
}

output "base_ami_id" {
  value = var.base_ami_id
}

output "public_route_table_id" {
  value = aws_route_table.public_subnet_rt.id
}

output "igw_id" {
  value = aws_internet_gateway.igw.id
}

output "nat_gw_id" {
  value = aws_nat_gateway.nat_gw.id
}

output "ipv6_cidr_block" {
  value = aws_vpc.vpc.ipv6_cidr_block
}

output "private_subnet_id" {
  value = aws_subnet.private_subnet.id
}

output "provider" {
  value = "aws"
}

output "public_subnet_id" {
  value = aws_subnet.public_subnet.id
}

output "redis_cluster_endpoint" {
  value = aws_elasticache_replication_group.redis_cluster.primary_endpoint_address
}

output "redis_cluster_id" {
  value = aws_elasticache_replication_group.redis_cluster.id
}

output "redis_cluster_password_key" {
  value = local.redis_password_key
}

output "redis_cluster_port" {
  value = aws_elasticache_replication_group.redis_cluster.port
}

output "redis_cluster_security_group_ids" {
  value = [for v in aws_elasticache_replication_group.redis_cluster.security_group_ids : v]
}

output "redis_cluster_user" {
  value = local.redis_user
}

output "region" {
  value = var.region
}

output "shard" {
  value = var.shard
}

output "sns_topic_arn" {
  value = aws_sns_topic.workload_sns_topic.arn
}

output "vpc_id" {
  value = aws_vpc.vpc.id
}

output "workload_sns_topic_arn" {
  value = aws_sns_topic.workload_sns_topic.arn
}

output "workload_vpc_cidr_block" {
  value = var.vpc_cidr_block
}

output "workload_vpc_id" {
  value = aws_vpc.vpc.id
}

output "workload_vpc_ipv6_cidr_block" {
  value = aws_vpc.vpc.ipv6_cidr_block
}

output "workload_vpc_private_subnet_cidr" {
  value = aws_subnet.private_subnet.cidr_block
}

output "workload_vpc_public_subnet_cidr" {
  value = aws_subnet.public_subnet.cidr_block
}
