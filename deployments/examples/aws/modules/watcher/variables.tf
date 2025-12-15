variable "resource_name_prefix" {
  type        = string
  description = "The prefix to add to the resource name"
}
variable "database_instance_id" {
  type        = string
  description = "The ID of the database instance whose primary db to monitor."
}
variable "vpc_subnet_ids" {
  type = list(string)
  description = "The list of subnet IDs to deploy the watcher in."
}
variable "vpc_sg_id" {
  type        = string
  description = "The ID of the security group for the watcher. This is used for guarentee the reachability of the watcher to the hostname."
}
variable "hostname_to_check" {
  type        = string
  description = "The hostname to resolve IP for"
}
variable "target_group_arn" {
  type        = string
  description = "The ARN of the target group to attach the IP Target to."
}
variable "schedule_expression" {
  type        = string
  description = "The schedule expression for the cronjob function."
}
variable "port" {
  type        = number
  description = "The port to check the hostname on."
}
variable "timeout" {
  type        = number
  description = "The timeout for this fucntion run."
}
variable "cross_vpc" {
  type        = string
  description = "Whehther the watcher needs to monitor peered-VPC addresses. true / false"
  default     = "true"
}
variable "sns_topic_arn" {
  type        = string
  description = "The ARN of the SNS topic to publish the watcher errors to."
}