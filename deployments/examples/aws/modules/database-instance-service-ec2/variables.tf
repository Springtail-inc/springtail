variable "resource_name_prefix" {
  type = string
}
variable "instance_ami_id" {
  type        = string
  description = "AMI ID for the instance. This is a custom AMI we pre-bake with necessary Springtail services."
}
variable "instance_type" {
  type        = string
  description = "Instance type for the instance."
}
variable "instance_subnet_id" {
  type        = string
  description = "Subnet ID for the instance."
}
variable "instance_security_group_ids" {
  type        = list(string)
  description = "Security group ID for the instance."
}
variable "user_data" {
  type        = string
  description = "Additional user data (script) to be passed to the instance for bootstrapping the instance for CloudInit. Any script passed in here will be *appended* to the default script."
  default     = ""
}
variable "instance_profile_name" {
  type        = string
  description = "Instance profile name for the instance."
  default     = null
}
variable "instance_keys" {
  type        = set(string)
  description = "The ID or keys of the instances to launch. Each EC2 instance launched is identified by a key. If not provided, no instance is launched."
  default     = []
}
variable "enable_primary_ipv6" {
  type        = bool
  description = "Whether to enable primary IPv6 for the instance."
  default     = false
}

variable "env_vars" {
  type        = map(string)
  description = "The environment variables to be set on the instance."
  default     = {}
}

variable "associate_public_ip_address" {
  type        = bool
  description = "Whether to associate a public IP address with the instance."
  default     = false
}

variable "volume_size" {
  type        = number
  description = "The size of the root volume in GB."
  default     = 32
}

variable "logging_volume" {
  type = object({
    mount_point = string
    device_name = string
    size        = number
    throughput  = number
    iops        = number
  })
  description = "A separate EBS volumes (tpye GP3) to attach to the instance for logging purpose. Note the device name must be unique and *cannot* be any of the following: sdf, sdg, sdh, sdk, sdl, as these are reserved for the ZFS RAID."
  default     = null
}

variable "service_config" {
  type = object({
    organization_id      = string
    account_id           = string
    database_instance_id = string
    region               = string
    shard = object({
      shard  = string # Shard ID, e.g. s1
      vpc_id = string # The ID of the workload VPC for this shard
      az_id  = string # The AZ that shard be launched in
      redis = object({
        cluster_endpoint   = string       # The DNS name for the ElastiCache cluster
        cluster_port       = number       # The port number for the ElastiCache cluster
        user               = string       # The user for the ElastiCache cluster
        password_key       = string       # The key to the password for the ElastiCache cluster as in SecretsManager
        security_group_ids = list(string) # The ID list of the security group for the ElastiCache cluster
      })
    })
  })
  description = "The service configuration. These are mandatory for the service to function properly."
}

variable "tags" {
  type        = map(string)
  description = "The tags to be applied to the instance."
  default     = {}
}

variable "primary_db_password_key" {
  type        = string
  description = "The SecretsManager key to the primary database password."
  default     = ""
}
