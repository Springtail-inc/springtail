#!/usr/bin/env bash
# Initializes the very basics for an EC2 instance, including
# the base env vars and setting up the ZFS pool (for ingestion instance)
# Only the fundamental Env vars are set here and the AWS - dependent initializations.

set -euo pipefail

# 1. Preliminary checks for tools we need
echo -n "Checking aws cli ... "
if ! command -v aws &> /dev/null; then
  echo "aws cli not installed."
  exit 1
fi
echo "ok"
echo -n "Checking redis-cli ... "
if ! command -v redis-cli &> /dev/null; then
  echo "redis-cli not installed."
  exit 1
fi
echo "ok"
echo -n "Checking and installing the aws-env ... "
if ! command -v aws-env &> /dev/null; then
  echo "aws-env not installed."
  exit 1
fi
echo "ok"
echo -n "Checking and installing the ec2tag ... "
if ! command -v ec2tag &> /dev/null; then
  echo "ec2tag not installed."
  exit 1
fi
echo "ok"

# 2. Set up fundamental Env Vars
source /etc/environment
# 2.1 Make the environment variables available in the current shell and its subprocesses
# the whole "(- xxx -)" block will be replaced with a lines, of such format as `key=value`
echo "Setting the environment variables"
_VARS='(-ENV_VARS_BLOCK_STRING-)'
$(aws-env -p -s -e <<< "$_VARS")

# 2.2 Write environment variables into the /etc/environment file
while IFS= read -r line; do
  echo "$line" | tee -a /etc/environment > /dev/null
done <<< $(aws-env -p -s <<< "$_VARS")
unset _VARS

# Check the env vars existence
if [[ -z "$DATABASE_INSTANCE_ID" ]]; then
  echo "DATABASE_INSTANCE_ID is not set."
  exit 1
fi
if [[ -z "$ORGANIZATION_ID" ]]; then
  echo "ORGANIZATION_ID is not set."
  exit 1
fi
if [[ -z "$ACCOUNT_ID" ]]; then
  echo "ACCOUNT_ID is not set."
  exit 1
fi
if [[ -z "$SERVICE_NAME" ]]; then
  echo "SERVICE_NAME is not set."
  exit 1
fi
if [[ -z "$MOUNT_POINT" ]]; then
  echo "MOUNT_POINT is not set."
  exit 1
fi

# 2.3 Sets the instance key env variable, querying the metadata.
INSTANCE_KEY=$(ec2tag InstanceKey)
export INSTANCE_KEY
echo "INSTANCE_KEY is set to $INSTANCE_KEY"
echo "INSTANCE_KEY=$INSTANCE_KEY" >> /etc/environment

# Amend some service specific environment variables
if [[ "$SERVICE_NAME" == "fdw" ]]; then
    # Sets FDW_ID to the INSTANCE_KEY
   	export FDW_ID="$INSTANCE_KEY"
   	echo "FDW_ID=$FDW_ID" >> /etc/environment
fi


# 3. Volumes and ZFS Pool setup (for ingestion instance)
# Depends on AWS Role to access own resources.
# Depends on the following env vars:
# - ZFS_POOL: the name of the ZFS pool to create (only for ingestion
#   instance)
# - MOUNT_POINT: the mount point for the ZFS pool, exposed via Ingestion to other instances.
# - NFS_SERVER_IP: the IP address of the NFS server (only for non ingestion instances)
# - LOGGING_VOLUME_MOUNT_POINT: the mount point for the logging volume (only
#   for proxy instance)


# Attach the logging volume to Proxy only
# LOGGING_VOLUME_MOUNT_POINT is only available in the proxy instance and when we use ZFS
if [[ "$SERVICE_NAME" == "proxy" ]]; then
  # Find the logging volume by Role tag ('logging')
  # Remove the 'vol-' prefix to replace it with 'vol'
  volid=$(aws ec2 describe-volumes \
    --filters "Name=tag:DatabaseInstanceID,Values=${DATABASE_INSTANCE_ID}" "Name=tag:OrganizationID,Values=${ORGANIZATION_ID}" "Name=tag:AccountID,Values=${ACCOUNT_ID}" "Name=tag:Role,Values=logging" \
    --query "Volumes[0].VolumeId" --output text | sed 's/vol-//g')
  devname="/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol${volid}"

  # wait up to 30 seconds for the symlink to show up
  # in very rare case the symlink is not created immediately (need to load the volume mapper driver)
  for i in $(seq 1 30); do
    if [ -e "$devname" ]; then
      echo "Found device at $devname"
      break
    fi
    echo "Waiting for $devname (attempt $i)"
    sleep 1
  done

  # Creates a partition (so we can grow it later easily)
  parted --script "$devname" mklabel gpt mkpart primary 0% 100%
  # Makes sure the kernel is aware of the new partition table
  partprobe "$devname"
  udevadm settle --exit-if-exists="${devname}-part1"
  # Format the partition with xfs
  mkfs.xfs -f -m reflink=1,crc=1 -l size=256m -L logging "${devname}-part1"

  if [[ -z "$LOGGING_VOLUME_MOUNT_POINT" ]]; then
    echo "LOGGING_VOLUME_MOUNT_POINT is not set. Make sure you are on the correct version of infra (post 2025-05-11)."
    exit 1
  fi
  mkdir -p "$LOGGING_VOLUME_MOUNT_POINT"

  # Update the /etc/fstab with the mount point
  echo "${devname}-part1 $LOGGING_VOLUME_MOUNT_POINT xfs  defaults,noatime,nodiratime,discard 0 2" | tee -a /etc/fstab
  systemctl daemon-reload
  mount "$LOGGING_VOLUME_MOUNT_POINT"
fi

# Create ZPOOL and share via NFS for ingestion instance
if [[ "$SERVICE_NAME" == "ingestion" ]]; then
  if [[ -z "$ZFS_POOL" ]]; then
    echo "ZFS_POOL is not set. Make sure you use the right AMI."
    exit 1
  fi
  # Map the ZFS volumes to the correct tags
  update_vdev_aliases
  # Mount the ZFS pool (RAID10) and SLOG
  systemctl enable nfs-server
  systemctl start nfs-server
  zpool create -o ashift=12 "$ZFS_POOL" mirror /dev/disk/by-vdev/zfs-raid10-0 /dev/disk/by-vdev/zfs-raid10-1 \
    mirror /dev/disk/by-vdev/zfs-raid10-2 /dev/disk/by-vdev/zfs-raid10-3
  zpool add "$ZFS_POOL" log /dev/disk/by-vdev/zfs-slog
  zfs set compression=off atime=off recordsize=32k mountpoint="${MOUNT_POINT}" "$ZFS_POOL"
  zpool set autoexpand=on "$ZFS_POOL"
  # Export NFS:
  # - root_squash: map root to nobody, so that the clients cannot write to the share for sure
  # - anonuid and anongid: map the anonymous user to the springtail user, so that bot the server and the client side see the same UID/GID for the path.
  zfs set sharenfs="rw,sync,no_subtree_check,root_squash,anonuid=$(id -u springtail),anongid=$(id -g springtail)" "$ZFS_POOL"
  exportfs -v
  # Ensure the mount point is owned by the springtail user
  chown -R springtail:springtail "${MOUNT_POINT}"
  systemctl restart nfs-server
else
  # Other instances mount the NFS share
  if [[ -z "$NFS_SERVER_IP" ]]; then
    echo "NFS_SERVER_IP is not set. Make sure you are on the correct version of infra (post 2025-05-11)."
    exit 1
  fi
  # Mount the NFS share only on other instances. We need to do a wait-for-it to ensure the NFS server is up and running.
  echo "${NFS_SERVER_IP}:${MOUNT_POINT} ${MOUNT_POINT} nfs4 defaults,vers=4.2,proto=tcp,hard,intr,nconnect=16,_netdev,rsize=1048576,wsize=1048576,timeo=600,retrans=2" >> /etc/fstab

  # Wait for the NFS server to be available, check every 15 seconds for 10 minutes
  MAX_ATTEMPTS=$((10*60/15))
  count=0
  until showmount -e "$NFS_SERVER_IP" &>/dev/null; do
    if [ "$count" -ge "$MAX_ATTEMPTS" ]; then
      echo "Timeout: NFS server did not become available after 5 minutes" >&2
      exit 1
    fi

    count=$((count + 1))
    echo "[$(date)] waiting for NFS server $NFS_SERVER_IP… ($count/$MAX_ATTEMPTS)"
    sleep 15
  done
  # MOUNT, try to mount the NFS share, 5 times, with a 30 seconds delay between attempts
  MAX_MOUNT_ATTEMPTS=5
  if mountpoint -q "$MOUNT_POINT"; then
    echo "[$(date -Is)] $MOUNT_POINT already mounted — nothing to do."
  else
    for i in $(seq 1 "$MAX_MOUNT_ATTEMPTS"); do
      echo "[$(date -Is)] mount attempt $i/$MAX_MOUNT_ATTEMPTS"

      if mount "$MOUNT_POINT"; then
        echo "[$(date -Is)] mounted $MOUNT_POINT successfully on attempt $i"
        break
      fi

      # Sleep only if another attempt will follow
      if [ "$i" -lt "$MAX_MOUNT_ATTEMPTS" ]; then
        sleep 30
      fi
    done
  fi
  # Eventually check if the mount was successful
  if ! mountpoint -q "$MOUNT_POINT"; then
    echo "Failed to mount ${MOUNT_POINT} after $MAX_MOUNT_ATTEMPTS attempts" >&2
  fi
fi

# Set a marker for successful initialization
echo '<SUCCESS>'