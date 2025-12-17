# Variables on application level, these are used for naming
# and tagging resources. These are the general information no matter what topology is.
# -- Begin: General info
variable "template_version" {
  type        = string
  description = "The version of the template from which this whole infra is created."
  default     = "v1.0.0"
}
variable "organization_id" {
  type        = string
  description = "The ID of the organization"
}
variable "account_id" {
  type        = string
  description = "The ID of the account as in Springtail, not the AWS account ID"
}
variable "environment" {
  type        = string
  description = "The environment"
}
variable "database_instance_id" {
  type        = string
  description = "The ID of the database instance. "
}

variable "region" {
  type        = string
  description = "The region where the database instance is launched."
}

# -- End: General info
# -- Begin: Customer info
variable "customer_database" {
  type = object({
    aws_account_id  = optional(string)
    region          = string
    vpc_id          = optional(string)
    vpc_cidr_blocks = optional(list(string))
    hostname        = optional(string)
    port            = optional(number)
  })
  description = "The customer database information. For public instance, we only need the 'region' information."
}
variable "customer_apps" {
  type = list(object({
    aws_account_id  = string
    region          = string
    vpc_id          = string
    vpc_cidr_blocks = list(string)
  }))
  description = "The list of the customer VPC information where their applications are hosted that consume Springtail services."
  default     = []
}
# -- End: customer info

# -- Begin: Springtail services

variable "instance_resource_access" {
  type = object({
    s3_access_arns             = optional(list(string))
    ecr_access_arns            = optional(list(string))
    sns_topic_publish_arn      = optional(string)
    sns_topic_subscribe_arns   = optional(list(string))
    secretsmanager_access_arns = optional(list(string))
  })
  description = "The configuration for the ingestion service."
}

variable "common_env_vars" {
  type        = map(string)
  description = <<-EOF
The additional common environment variables for the all Springtail services. These vars will be set system-wide to EC2 instance
and ECS tasks. You can stuff in more env vars other than the built-in ones. You can put sensitive variables into SecretsManager and
prepended the Secret ID (Not the ARN) with "secretsmanager://". Then this variable will be replaced by the value fetched from Secrets Manager
at container or instance bootstrap phase.
EOF
  default     = {}
}

variable "custom_domain_name" {
  type        = string
  description = "The custom domain name to use for the access to our Springtail DB Service."
}
variable "proxy_service" {
  type = object({
    instance_keys = optional(set(string), [])
    machine_image = string
    instance_type = string
    env_vars      = optional(map(string), {})
    user_data     = optional(string, "")
    volume_size   = optional(number, 40)

    target_port       = number
    health_check_port = number

    # This is the port the customer will connect to (proxy) in order to access Springtail service.
    nlb_listener_port = number
  })
  description = "The configuration for the ingestion service."
}

variable "fdw_service" {
  type = object({
    instance_keys = optional(set(string), [])
    machine_image = string
    instance_type = string
    env_vars      = optional(map(string), {})
    user_data     = optional(string, "")
    volume_size   = optional(number, 40)
  })
  description = "The configuration for the ingestion service."
}

variable "ingestion_service" {
  type = object({
    instance_keys = optional(set(string), [])
    machine_image = string
    instance_type = string
    env_vars      = optional(map(string), {})
    user_data     = optional(string, "")
    volume_size   = optional(number, 40)
  })
  description = "The configuration for the ingestion service."
}

variable "zfs" {
  type = object({
    per_volume_size = number
    iops            = number
    throughput      = number
    slog_size       = number
  })
  description = "The ZFS-NFS configuration info. If null, do not use ZFS-NFS."
  default     = null
}

# -- End: Springtail services

# -- Begin: Shard specific info
variable "shard" {
  type = object({
    shard                 = string # Shard ID, e.g. s1
    vpc_id                = string # The ID of the workload VPC for this shard
    az_id                 = string # The AZ that shard be launched in
    vpc_cidr_block        = string # The CIDR block of the workload VPC for this shard
    vpc_ipv6_cidr_block   = string # The IPv6 CIDR block of the workload VPC for this shard
    public_route_table_id = string

    redis = object({
      cluster_endpoint   = string       # The DNS name for the ElastiCache cluster
      cluster_port       = number       # The port number for the ElastiCache cluster
      user               = string       # The user for the ElastiCache cluster
      password_key       = string       # The key to the password for the ElastiCache cluster as in SecretsManager
      security_group_ids = list(string) # The ID list of the security group for the ElastiCache cluster
    })

    sns_topic_arn = string # The ARN of the SNS topic for infrastructure level operations to use
  })
  description = "The shard specific information."
}
# -- End: Shard specific info




