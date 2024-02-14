#!/bin/bash
SPRINGTAIL_SRC="../"

cd $SPRINGTAIL_SRC

if command -v docker-buildx >/dev/null 2>&1; then
    docker-buildx build --progress plain -t springtail:base -f ./docker/Dockerfile.base .
else
    docker buildx build --progress plain -t springtail:base -f ./docker/Dockerfile.base .
fi
