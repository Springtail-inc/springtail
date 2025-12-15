region          = "us-east-1"
shard           = "s1"
base_ami_id     = "ami-071e80e19cf59c0e8" # Set this to the base image with common deps installed
az_id           = "use1-az1"              # AZ where the shard will be launched
vpc_cidr_block  = "10.1.0.0/16"
assume_role_arn = "arn:aws:iam::123456789012:role/YourAssumeRole" # ARN of the role to assume
