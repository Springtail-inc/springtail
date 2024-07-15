#!/bin/bash

# capture the script arguments
BUILD_DIR=$1

# stop postgres
brew services stop postgresql@16

# install the FDW extension into postgres
SHARE_DIR=`pg_config --sharedir`
LIB_DIR=`pg_config --libdir`

cp ${BUILD_DIR}/../src/pg_fdw/springtail_fdw--1.0.sql ${SHARE_DIR}/extension/
cp ${BUILD_DIR}/../src/pg_fdw/springtail_fdw.control ${SHARE_DIR}/extension/
cp ${BUILD_DIR}/src/pg_fdw/libspringtail_pg_fdw.dylib ${LIB_DIR}/postgresql/springtail_fdw.dylib
cd ${LIB_DIR}/postgresql/
dsymutil springtail_fdw.dylib

# start postgres
brew services start postgresql@16
sleep 1
