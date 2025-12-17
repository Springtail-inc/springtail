variable "resource_name_prefix" {
  type        = string
  description = "The prefix to add to the resource name"
}
variable "vpc_id" {
  type        = string
  description = "The ID of the VPC where the NLB will be created."
}
variable "enable_cross_zone_load_balancing" {
  type        = bool
  description = "Whether to enable cross-zone load balancing"
  default     = true
}
variable "subnet_ids" {
  type = list(string)
  description = "The List of ID of the subnets for the NLB"
}
variable "internal" {
  type        = bool
  description = "Whether the NLB is internal or not"
  default     = true
}

# For NLB, a listener listens on a port and fwd the traffic to a target group
variable "target_config" {
  type = map(object({
    listener_port       = number
    target_port         = number
    protocol            = string
    interval            = number
    unhealthy_threshold = number
    ingress_cidr_blocks = list(string)
    type                = string  # ip or instance
    health_check_port   = optional(number, 0)
  }))
  description = "The configuration for the target group."
}
