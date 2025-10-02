#!/bin/bash
# Redirect the EC2 meta endpoint to Controller box.
CONTROLLER_IP=$(getent ahostsv4  controller | awk '{print $1; exit}')
if [ -z "$CONTROLLER_IP" ]; then
  echo "Failed to resolve 'controller' hostname. Is the local cluster running?"
  exit 1
fi
iptables -t nat -A OUTPUT -p tcp -d 169.254.169.254 --dport 80 -j DNAT --to-destination ${CONTROLLER_IP}:${CONTROLLER_PORT}

# Make sure that the logs path is writable
mkdir -p "${INSTALL_DIR}"/logs && chmod 777 "${INSTALL_DIR}"/logs
mkdir -p "${INSTALL_DIR}"/pids && chmod 777 "${INSTALL_DIR}"/pids
chown -R springtail:springtail "${INSTALL_DIR}"/logs "${INSTALL_DIR}"/pids

# If the /shadow_logs directory exists, make it owned by springtail user

if [ -d /shadow_logs ]; then
  chmod 777 /shadow_logs
  chown -R springtail:springtail /shadow_logs
fi

# Download the package from S3
echo "Installing the boostrap coordinator package"
if [ -z "$PACKAGE_FILE_NAME" ]; then
  echo "PACKAGE_FILE_NAME variable not set in env.setup"
  exit 1
fi
if [ ! -f /tmp/"$PACKAGE_FILE_NAME" ]; then
  if [ -z "$S3_BUCKET" ]; then
    echo "S3_BUCKET variable not set in env.setup"
    exit 1
  fi

  PACKAGE_FILE_BASE_NAME=$(basename "$PACKAGE_FILE_NAME")
  echo "Downloading package $PACKAGE_FILE_BASE_NAME from S3 bucket $S3_BUCKET ..."
  aws --endpoint-url=http://aws-mock:7000 s3 cp "s3://$S3_BUCKET/packages/$PACKAGE_FILE_BASE_NAME" /tmp/"$PACKAGE_FILE_BASE_NAME"
  if [ $? -ne 0 ]; then
    echo "Failed to download package $PACKAGE_FILE_BASE_NAME from S3 bucket $S3_BUCKET"
    exit 1
  fi
  # Extract the package and move the `python` folder to"${INSTALL_DIR}"/stc
  tar -xzf /tmp/"$PACKAGE_FILE_BASE_NAME" -C /tmp/
  if [ $? -ne 0 ]; then
    echo "Failed to extract package $PACKAGE_FILE_BASE_NAME"
    exit 1
  fi
  rm -rf ${INSTALL_DIR}/stc
  mv /tmp/python ${INSTALL_DIR}/stc
  if [ $? -ne 0 ]; then
    echo "Failed to move extracted package to ${INSTALL_DIR}/stc"
    exit 1
  fi
fi
# Generate the production config
echo "install_dir: '${INSTALL_DIR}'" > ${INSTALL_DIR}/stc/coordinator/config.yaml
echo "production: True" >> ${INSTALL_DIR}/stc/coordinator/config.yaml
echo "log_rotation_size: 104857600" >> ${INSTALL_DIR}/stc/coordinator/config.yaml
echo "log_rotation_count: 10" >> ${INSTALL_DIR}/stc/coordinator/config.yaml

if [ "$SERVICE_NAME" == "fdw" ]; then
  echo "Starting the bootstrap FDW Custom PG"
  CRYPTOGRAPHY_OPENSSL_NO_LEGACY=1 ansible-playbook -i localhost, -c local "${INSTALL_DIR}/customize-pg.yml" --extra-vars "username=${FDW_USER}"
fi
echo "<SUCCESS> Bootstrap coordinator package installed"