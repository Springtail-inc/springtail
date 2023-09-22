#!/bin/bash
if [ ! -d debug ]; then
    mkdir -p debug
    cmake -B debug -S . -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=${VCPKG_SRC}/scripts/buildsystems/vcpkg.cmake
fi
cd debug
make VERBOSE=1 $1 $2
