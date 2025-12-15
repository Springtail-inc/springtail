variable "vpc_id" {
  type        = string
  description = "The VPC ID"
}
variable "vpc_endpoint_service_name" {
  type        = string
  description = "The VPC endpoint service name"
}
variable "inbound_cidr_blocks" {
  type = list(string)
  description = "The CIDR block that is allowed to access this VPC endpoint"
}
variable "subnet_ids" {
  type = list(string)
  description = "The subnet IDs"
}
variable "name" {
  type        = string
  description = "The name tag of the VPC endpoint"
}
variable "custom_domain" {
  type = object({
    base_domain_name = string
    sub_domain_name  = string
    # The initial VPC to attach to the this custom domain when
    # creating the private hosted zone.
    initial_vpc = object({
      vpc_id     = string
      vpc_region = string
    })
    resolvable_vpcs = optional(list(object({
      vpc_id     = string
      vpc_region = string
    })), [])
  })
  description = <<-EOF
The domain name and Zone ID for th VPC endpoint. If not provided, the default AWS generated name will be used.
The Zone ID is normally the output from the shard infra deployment. The resolvable_vpcs is the list of VPC info
that can resolve this domain name.
EOF
  default     = null
}