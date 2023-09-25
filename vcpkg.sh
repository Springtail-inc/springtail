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
configfile="../../vcpkg_files.txt"
while read -r line; do
   [[ "$line" =~ ^#.*$ ]] && continue
   ./vcpkg install $line
done <$configfile

