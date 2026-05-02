#!/bin/bash

# Start the PostgreSQL service
service postgresql start

# Start the Redis service
service redis-server start

# Create a PostgreSQL superuser with login permissions
sudo -u postgres psql -c "CREATE USER springtail WITH PASSWORD 'springtail' SUPERUSER LOGIN";

export ZIC=true
export VCPKG_FORCE_SYSTEM_BINARIES=1

# Start the SSH service
echo "Starting SSH..."
service ssh start

# If a command was passed, run it; otherwise keep the container running
if [ $# -gt 0 ]; then
    exec "$@"
else
    tail -f /dev/null
fi
