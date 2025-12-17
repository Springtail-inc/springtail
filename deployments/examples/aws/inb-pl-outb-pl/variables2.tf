# Variables specific to this inbound-private-link and outbound-private-link topology
# -- Begin: networking_config
variable "networking" {
  type = object({
    transit_vpc = object({
      cidr_block = string
    })
    database_instance_subnet = object({
      cidr_block = string
      nat_gw_id  = string
    })
  })
  description = "The networking configuration."
}
# -- End: networking_config

# -- Begin: Watcher
variable "watcher" {
  type = object({
    # CloudWatch Event only support the minimal 1 minute precision.
    schedule_expression = string
    # Timeout
    timeout = number
  })
  description = "The watcher configuration."
}
# -- End: Watcher
