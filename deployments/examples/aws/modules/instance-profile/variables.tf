variable "resource_name_prefix" {
  type = string
}
variable "s3_access_arns" {
  type        = list(string)
  description = "List of S3 ARNs to allow access (read/write) to. No access to S3 if not provided or null."
  default     = null
}
variable "sns_topic_subscribe_arns" {
  type        = list(string)
  description = "List of SNS ARNs to allow subscribe to. No access to SNS if not provided or null."
  default     = null
}
variable "sns_topic_publish_arns" {
  type        = list(string)
  description = "List of SNS ARNs to allow publish to. No access to SNS if not provided or null."
  default     = null
}
variable "ecr_access_arns" {
  type        = list(string)
  description = "List of ECR ARNs to allow access (Pull/Push). No access to ECR if not provided or null."
  default     = null
}
variable "secretsmanager_access_arns" {
  type        = list(string)
  description = "List of Secrets Manager ARNs to allow access."
  default     = null
}
variable "step_function_arns" {
  type        = list(string)
  description = "List of Step Function ARNs to allow execution and queries."
  default     = null
}
