#!/bin/bash

if [ $(uname -p) == aarch64 ]; then
    export VCPKG_FORCE_SYSTEM_BINARIES=1
fi

# install dependent packages with vcpkg
./vcpkg.sh

# setup the debug build
if [ ! -d debug ]; then
    mkdir -p debug
    cmake -B debug -S . -DCMAKE_BUILD_TYPE=Debug
fi

# build the code
cd debug
make VERBOSE=1 $1 $2
