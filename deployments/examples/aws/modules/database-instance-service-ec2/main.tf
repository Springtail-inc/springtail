locals {
  dbi_region = var.service_config.region
  # Preamble is a template file
  cloudinit_script = replace(
    replace(
      file("${path.module}/scripts/init.sh.tpl"),
      "(-ENV_VARS_BLOCK_STRING-)",
      # Converts the map of env vars to a string of key=value pairs format
      join("\n", [
        for key, value in local.combined_env_vars :
        format("%s=%s", key, value)
      ])
    ),
    "(-AWS_REGION-)",
    local.dbi_region
  )

  default_user_data = local.cloudinit_script
}

# We use `instance_keys` to launch multiple instances with the same configuration.
# This way we have precise control over which instance to terminate or stop. This is useful for
# FDW scaling, in that we can have a predefined set of IDs to identify the instances.
# Changing instance keys will cause the instance to be recreated.
resource "aws_instance" "services_instance" {
  for_each                    = var.instance_keys
  subnet_id                   = var.instance_subnet_id
  ami                         = var.instance_ami_id
  associate_public_ip_address = var.associate_public_ip_address
  instance_type               = var.instance_type
  iam_instance_profile        = var.instance_profile_name
  vpc_security_group_ids      = var.instance_security_group_ids
  user_data = join("\n", [local.default_user_data, var.user_data])
  user_data_replace_on_change = true
  monitoring                  = true
  placement_partition_number  = 0
  enable_primary_ipv6         = var.enable_primary_ipv6

  disable_api_stop                     = false
  disable_api_termination              = false
  ebs_optimized                        = true
  hibernation                          = false
  instance_initiated_shutdown_behavior = "stop"
  secondary_private_ips = []

  root_block_device {
    volume_size = var.volume_size
    volume_type = "gp3"
  }
  tags = merge({
    Name        = "${var.resource_name_prefix}-${each.value}"
    InstanceKey = each.value
  }, var.tags)

  # Avoid to recreate the instance as much as possible.
  lifecycle {
    create_before_destroy = true
    ignore_changes = [instance_type, user_data]
  }
  metadata_options {
    instance_metadata_tags = "enabled"
  }
}
# Additional Volumes
resource "aws_ebs_volume" "logging_volume" {
  for_each          = var.logging_volume != null ? toset(["1"]) : toset([])
  availability_zone = values(aws_instance.services_instance)[0].availability_zone
  size              = var.logging_volume.size
  type              = "gp3"
  throughput        = var.logging_volume.throughput
  iops = var.logging_volume.iops
  # Hard coded Tag Role : logging, so in the bootstrap we can identify the volume
  tags = merge({ Role = "logging" }, var.tags)
  final_snapshot    = false
}

resource "aws_volume_attachment" "logging_volume_attachment" {
  for_each     = var.logging_volume != null ? var.instance_keys : toset([])
  device_name  = "/dev/sdi"
  volume_id    = values(aws_ebs_volume.logging_volume)[0].id
  instance_id  = aws_instance.services_instance[each.key].id
  force_detach = true
}


# -- Output --
output "instance_ids" {
  value = {for i in var.instance_keys : i => aws_instance.services_instance[i].id}
}
output "instance_ips" {
  value = {for i in var.instance_keys : i => aws_instance.services_instance[i].private_ip}
}

output "public_ips" {
  value = {for i in var.instance_keys : i => aws_instance.services_instance[i].public_ip}
}

output "ipv6_public_ips" {
  value = {
    for i in var.instance_keys :
    i => aws_instance.services_instance[i].ipv6_addresses[0]
    if length(aws_instance.services_instance[i].ipv6_addresses) > 0
  }
}

output "instance_keys" {
  value = var.instance_keys
}

output "instance_availability_zone" {
  value = ((aws_instance.services_instance != null && length(aws_instance.services_instance) > 0) ?
    values(aws_instance.services_instance)[0].availability_zone : null)
}
# -- This is for reference purpose. Output *all* resource ARNs for this Module.
output "all_resource_ids" {
  value = {
    instance_ids = [for k, v in aws_instance.services_instance : v.id]
  }
}