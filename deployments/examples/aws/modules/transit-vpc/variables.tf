variable "resource_name_prefix" {
  type        = string
  description = "The prefix to add to the resource name"
}
variable "cidr_block" {
  type        = string
  description = "The CIDR block for the VPC. Need to ensure the mask is at most /23."
}
variable "az_ids" {
  type = list(string)
  description = "List of all AZ IDs we want to cover, including primary AZ and all failover AZs"
}
variable "database_vpc_peering" {
  type = object({
    aws_account_id = string
    region         = string
    vpc_id         = string
    cidr_blocks = list(string)
  })
  description = <<-EOF
  The VPC peering information about the VPC where the primary database is launched in.
EOF
}
variable "application_vpc_peerings" {
  type = list(object({
    aws_account_id = string
    region         = string
    vpc_id         = string
    cidr_blocks = list(string)
  }))
  description = <<-EOF
  The VPC peering information about the VPCs where the customer applications are launched in, which need to consume
the Springtail service.
EOF
}