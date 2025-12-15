variable "resource_name_prefix" {
  type        = string
  description = "The prefix to add to the resource name"
}

variable "vpc_id" {
  type        = string
  description = "The ID of the VPC where the watcher will be deployed."
}
variable "az_id" {
  type        = string
  description = "The ID of the availability zone where the subnet is based in."
}

variable "subnet_cidr_block" {
  type        = string
  description = "The CIDR block of the subnet."
}

variable "allowed_sg_ids" {
  type = list(string)
  description = "The IDs of the security groups for resourdes that are allowed to have bidirectional traffic. Used for building the output security group."
}

variable "nat_gw_id" {
  type        = string
  description = "The ID of the NAT Gateway to be used for the subnet route table."
}