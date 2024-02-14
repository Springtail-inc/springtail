#!/bin/bash
SPRINGTAIL_SRC="../"

cd $SPRINGTAIL_SRC

if command -v docker-buildx >/dev/null 2>&1; then
    docker-buildx build --progress plain -t springtail:exec -f ./docker/Dockerfile --no-cache .
else
    docker buildx build --progress plain -t springtail:exec -f ./docker/Dockerfile --no-cache .
fi
