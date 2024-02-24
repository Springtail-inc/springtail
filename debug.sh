#!/bin/bash

if [ $(uname -p) == aarch64 ]; then
    export VCPKG_FORCE_SYSTEM_BINARIES=1
fi

# install dependent packages with vcpkg
./vcpkg.sh

# setup the debug build
if [ ! -d debug ]; then
    mkdir -p debug
fi
cmake -B debug -S . -D'CMAKE_BUILD_TYPE=Debug'

# build the code
cd debug
make $1 $2
