#!/bin/bash

DIR=external/vcpkg

if [ ! -d ${DIR} ]
then
    if [[ $(uname -s) == "Darwin" ]]; then
        echo "Running on macOS"
        export CC=`which gcc-13`
        export CXX=`which g++-13`
    else
        echo "Not running on macOS"
        export CC=`which gcc`
        export CXX=`which g++`
    fi

    git clone https://github.com/Microsoft/vcpkg.git "${DIR}"
    cd "$DIR"
   ./bootstrap-vcpkg.sh
   ./vcpkg integrate install
else
   cd "$DIR"
fi
