# Create EBS volumes and attach them to EC2 instances
locals {
  ending_chars = ["f", "g", "h", "k"]
}

# SLOG Volume
resource "aws_ebs_volume" "zil_slog_ebs_volume" {
  size              = var.slog_size
  availability_zone = var.availability_zone
  type              = "gp3"
  encrypted         = true
  iops              = var.iops
  throughput        = var.throughput
  final_snapshot    = false
  tags = {
    Role = "zfs-slog"   # <- this is what we’ll map to a symlink
  }
}

# 4 volumes for RAID10
resource "aws_ebs_volume" "ebs_volumes" {
  count             = 4
  size              = var.per_volume_size
  availability_zone = var.availability_zone
  type              = "gp3"
  encrypted         = true
  iops              = var.iops
  throughput        = var.throughput
  final_snapshot    = false
  tags = {
    Role = "zfs-raid10-${count.index}"   # This is what we’ll map to a symlink
  }
}

resource "aws_volume_attachment" "slog_ebs_attachment" {
  device_name  = "/dev/sdl"
  instance_id  = var.instance_id
  volume_id    = aws_ebs_volume.zil_slog_ebs_volume.id
  force_detach = true
}

resource "aws_volume_attachment" "ebs_volumes_attachment" {
  count        = 4
  device_name  = "/dev/sd${local.ending_chars[count.index]}"
  instance_id  = var.instance_id
  volume_id    = aws_ebs_volume.ebs_volumes[count.index].id
  force_detach = true
}