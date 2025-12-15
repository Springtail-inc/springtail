variable "assume_role_arn" {
  type        = string
  description = "The ARN of the role to assume."
}

variable "az_id" {
  type        = string
  description = "The AZ that shard be launched in."
}

variable "base_ami_id" {
  type        = string
  description = "The base AMI ID."
}

variable "redis_node_type" {
  type        = string
  description = "The node type of the Redis cluster. E.g., cache.t3.small"
  default     = "cache.t3.small"
}


variable "region" {
  type        = string
  description = "The region name. E.g., us-east-1, us-west-2."
}

variable "shard" {
  type        = string
  description = "The shard name. E.g., s1, s2, s3."
}

variable "vpc_cidr_block" {
  type        = string
  description = "The workload VPC CIDR block."
}

variable "zfs" {
  type = object({
    per_volume_size = number
    iops            = number
    throughput      = number
    slog_size       = number
  })
  description = "ZFS configuration with some default values. Set to null to disable ZFS explicitly, otherwise it is enabled."
  default = {
    per_volume_size = 25
    iops            = 3000
    throughput      = 125
    slog_size       = 4
  }
}
