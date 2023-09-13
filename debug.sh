#!/bin/bash
if [ ! -d debug ]; then
    mkdir -p debug
    cmake -B debug -S . -DCMAKE_BUILD_TYPE=Debug
fi
cd debug
make VERBOSE=1 $1 $2
