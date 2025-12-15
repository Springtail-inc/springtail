# The `mgmt_vpc` is for deploying the management plane resources such as API.

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

resource "aws_vpc" "vpc" {
  cidr_block                       = var.vpc_cidr_block
  enable_dns_support               = true
  enable_dns_hostnames             = true
  assign_generated_ipv6_cidr_block = true
  tags = {
    Name = "${var.shard}-vpc"
  }
}
# -- Begin: Private Subnet --
resource "aws_subnet" "private_subnet" {
  vpc_id               = aws_vpc.vpc.id
  cidr_block           = cidrsubnet(var.vpc_cidr_block, 8, 0)
  availability_zone_id = var.az_id

  tags = {
    Name = "${var.shard}-vpc-private-subnet"
    AZID = var.az_id
  }
}

resource "aws_route_table" "private_subnet_rt" {
  vpc_id = aws_vpc.vpc.id
  tags = {
    Name = "${var.shard}-vpc-private-nat-rt"
    AZID = var.az_id
  }
}

# Point internet egress traffic to the NAT Gateway
resource "aws_route" "private_subnet_egress_route" {
  route_table_id         = aws_route_table.private_subnet_rt.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.nat_gw.id
}

# Link rt table to private subnet
resource "aws_route_table_association" "private_subnet_rt_assoc" {
  subnet_id      = aws_subnet.private_subnet.id
  route_table_id = aws_route_table.private_subnet_rt.id
}

# -- End: Private Subnet --

# -- Begin: Public Subnet --
resource "aws_subnet" "public_subnet" {
  vpc_id               = aws_vpc.vpc.id
  cidr_block           = cidrsubnet(var.vpc_cidr_block, 8, 1)
  availability_zone_id = var.az_id

  tags = {
    Name = "${var.shard}-vpc-public-subnet"
    AZID = var.az_id
  }
}

# Add an internet gateway to the VPC
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.vpc.id

  tags = {
    Name = "${var.shard}-vpc-igw"
  }
}

# Add a route table with a default route for the public subnet
resource "aws_route_table" "public_subnet_rt" {
  vpc_id = aws_vpc.vpc.id

  tags = {
    Name = "${var.shard}-vpc-public-rt"
  }
}

resource "aws_route" "public_subnet_rt_default_route" {
  route_table_id         = aws_route_table.public_subnet_rt.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.igw.id
}

resource "aws_route" "public_subnet_rt_ipv6_default_route" {
  route_table_id              = aws_route_table.public_subnet_rt.id
  destination_ipv6_cidr_block = "::/0"
  gateway_id                  = aws_internet_gateway.igw.id
}

# Associate the route table with the public subnet
resource "aws_route_table_association" "public_subnet_rt_assoc" {
  subnet_id      = aws_subnet.public_subnet.id
  route_table_id = aws_route_table.public_subnet_rt.id
}

# -- End: Public Subnet --

# -- NAT Gateway --
# Create Elastic IP for NAT Gateway
resource "aws_eip" "nat_eip" {
  domain = "vpc"
  tags = {
    Name = "${var.shard}-nat-eip"
    AZID = var.az_id
  }
}

resource "aws_nat_gateway" "nat_gw" {
  allocation_id = aws_eip.nat_eip.id
  subnet_id     = aws_subnet.public_subnet.id
  tags = {
    Name = "${var.shard}-nat-gw"
    AZID = var.az_id
  }
}

# Generates a 4 character random string
resource "random_string" "name_randomizer_4" {
  length           = 5
  upper            = true
  lower            = true
  override_special = "_-"
}
# Generates an 8 character random string
resource "random_string" "name_randomizer_8" {
  length           = 8
  upper            = true
  lower            = true
  override_special = "_-"
}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  name_suffix = "${local.region}_${var.shard}"
  region      = data.aws_region.current.region
  # This is generated
  redis_password_key = "redis/password/${local.region}/${var.shard}/${random_string.name_randomizer_4.result}"
  redis_user         = random_string.name_randomizer_8.result
}

# -- Redis cluster
resource "aws_elasticache_subnet_group" "redis_subnet_group" {
  name       = "${var.shard}-redis-subnet-grp"
  subnet_ids = [aws_subnet.private_subnet.id]
}
resource "aws_security_group" "redis_sg" {
  name        = "${var.shard}-redis-sg-${local.name_suffix}"
  description = "Security group for Redis cluster"
  vpc_id      = aws_vpc.vpc.id
  tags = {
    Name = "${var.shard}-redis-sg-${local.name_suffix}"
  }
}
# Allows inbound from this VPC CIDR
resource "aws_vpc_security_group_ingress_rule" "sg_ingress_rule" {
  security_group_id = aws_security_group.redis_sg.id
  ip_protocol       = "tcp"
  from_port         = 6379
  to_port           = 6379
  cidr_ipv4         = var.vpc_cidr_block
}

# Allows outbound to this VPC CIDR
resource "aws_vpc_security_group_egress_rule" "sg_egress_rule" {
  security_group_id = aws_security_group.redis_sg.id
  ip_protocol       = "-1"
  cidr_ipv4         = var.vpc_cidr_block
}

# -- Begin: Create a user and user group for Redis.
resource "random_password" "redis_password" {
  length           = 24
  special          = true
  lower            = true
  upper            = true
  numeric          = true
  override_special = "_-"
  keepers = {
    # Ensure the password is regenerated when the length is changed
    length = 24
    # Ensure the password is regnerated when the user name is changed
    user = local.redis_user
  }
}
# Put the password in AWS SecretsManager for later use
resource "aws_secretsmanager_secret" "redis_secret" {
  name = local.redis_password_key
}
resource "aws_secretsmanager_secret_version" "redis_secret_version" {
  secret_id     = aws_secretsmanager_secret.redis_secret.id
  secret_string = random_password.redis_password.result
}
resource "aws_elasticache_user" "redis_user" {
  user_name     = local.redis_user
  engine        = "redis"
  user_id       = "shared-user"
  access_string = "on ~* +@all -flushdb -flushall"

  authentication_mode {
    type      = "password"
    passwords = [random_password.redis_password.result]
  }
}
resource "aws_elasticache_user_group" "redis_user_group" {
  user_group_id = "default-shared-user-group"
  engine        = "redis"
  user_ids = [
    # ElastiCache automatically configures a default user with a user name "default"
    # and adds it to all User Groups. You can't modify or delete this user.
    "default",
    aws_elasticache_user.redis_user.id
  ]
}
# -- End: Create a user and user group for Redis.

# -- Begin: Redis parameters
resource "aws_elasticache_parameter_group" "redis_notify" {
  name   = "redis-ake-params"
  family = "redis7"

  parameter {
    name  = "notify-keyspace-events"
    value = "AKE"
  }
}

# -- End: Redis parameters

resource "aws_elasticache_replication_group" "redis_cluster" {
  replication_group_id = "rg-${data.aws_region.current.region}-${var.shard}"
  node_type            = var.redis_node_type
  engine               = "redis"
  description          = "shared cluster ${local.name_suffix}"
  # https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/SelectEngine.html
  engine_version             = "7.1"
  port                       = 6379
  subnet_group_name          = aws_elasticache_subnet_group.redis_subnet_group.name
  security_group_ids         = [aws_security_group.redis_sg.id]
  multi_az_enabled           = false
  automatic_failover_enabled = false
  num_cache_clusters         = 1
  snapshot_retention_limit   = 1
  snapshot_window            = "00:00-01:00"
  # https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/elasticache_replication_group#user_group_ids
  user_group_ids = [aws_elasticache_user_group.redis_user_group.id]
  #  User group based access control requires encryption-in-transit to be enabled on the replication group
  transit_encryption_enabled = true
  transit_encryption_mode    = "required"
  apply_immediately          = true
  parameter_group_name       = aws_elasticache_parameter_group.redis_notify.name
  tags = {
    Name = "${var.shard}-redis-cluster-${local.name_suffix}"
  }
}


# The SNS topic for handling events within this workload (Shard)
resource "aws_sns_topic" "workload_sns_topic" {
  name = "${var.shard}-sns-events"
}

# Only allow resources inside the same AWS account to publish and subscribe to the topic
resource "aws_sns_topic_policy" "config_change_topic_policy" {
  arn = aws_sns_topic.workload_sns_topic.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "*"
        }
        Action = [
          "sns:Publish",
          "sns:Subscribe",
        ],
        Resource = "*",
        Condition = {
          StringEquals = {
            "AWS:SourceOwner" = local.account_id
          }
        }
      }
    ]
  })
}