#!/bin/bash

set -e

if [ $(uname -p) == aarch64 ]; then
    export VCPKG_FORCE_SYSTEM_BINARIES=1
fi

# install dependent packages with vcpkg
./vcpkg.sh

if [ -e '/.dockerenv' ]; then
    DOCKER=1
else
    DOCKER=0
fi

# setup the debug build
if [ ! -d debug ]; then
    if [ $DOCKER -eq 1 ]; then
        echo "Building inside a container; symlinking debug dir"
        mkdir -p /home/dev/debug
        mkdir -p /home/dev/install
        ln -s /home/dev/debug debug
        ln -s /home/dev/install install
    else
        mkdir -p debug
    fi
fi

cmake -B debug -S . \
	-D'CMAKE_BUILD_TYPE=Debug' \
	-DVCPKG_INSTALL_OPTIONS="--allow-unsupported"

# build the code
cd debug
if command -v nproc >/dev/null 2>&1; then
    ncpus=$(( $(nproc) / 2 ))
    ncpus=$(( ncpus > 0 ? ncpus : 1 ))
else
    ncpus=2
fi
make -j${ncpus} $1 $2
