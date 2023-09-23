#!/bin/bash

# install dependent packages with vcpkg
./vcpkg.sh

# setup the debug build
if [ ! -d debug ]; then
    mkdir -p debug
    cmake -B debug -S . -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=external/vcpkg/scripts/buildsystems/vcpkg.cmake
fi

# build the code
cd debug
make VERBOSE=1 $1 $2
