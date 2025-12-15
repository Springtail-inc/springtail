variable "instance_id" {
  type        = string
  description = "The ID of the EC2 instance to attach the EBS volumes to."
}
variable "availability_zone" {
  type        = string
  description = "The availability zone of the EC2 instance. Note this is not the ID."
}
variable "per_volume_size" {
  type        = number
  description = "The size of each EBS volume in GiB. Minimum 25 GiB each."
  default     = 25
}
variable "iops" {
  type        = number
  description = "The IOPS of each EBS volume. Start with the default IOPS of 3,000 and 125MB/s throughput."
  default     = 3000
}
variable "throughput" {
  type        = number
  description = "The throughput of each EBS volume in MB/s.   # Start with the default IOPS of 3,000 and 125MB/s throughput."
  default     = 125
}
variable "slog_size" {
  type        = number
  description = "The size of the slog volume in GiB."
  default     = 4
}