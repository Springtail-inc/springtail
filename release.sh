#!/bin/bash
if [ ! -d release ]; then
    mkdir -p release
    cmake -B release -S . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DNDEBUG=1
fi
cd release
make VERBOSE=1 $1 $2
