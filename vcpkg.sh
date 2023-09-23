#!/bin/bash

DIR=external/vcpkg

if [ ! -d ${DIR} ]
then
   git clone https://github.com/Microsoft/vcpkg.git "${DIR}"
   cd "$DIR"
   ./bootstrap-vcpkg.sh
   ./vcpkg integrate install
else
   cd "$DIR"
fi

# install packages
./vcpkg install minio-cpp
./vcpkg install libpq
./vcpkg install boost-program-options
./vcpkg install fmt
