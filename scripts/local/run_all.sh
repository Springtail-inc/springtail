#!/bin/bash

BUILD_DIR=`pwd`/$1
echo ${BUILD_DIR}

PSQL_CMD="psql postgresql://springtail:springtail@localhost/springtail"
PSQL_CMD_REPLICA="psql postgresql://springtail:springtail@localhost/springtail_replica"

# change to the directory of the script
cd $(dirname $0)

# clear any previously existing state from the database
${PSQL_CMD} < cleanup.sql

# clear any previous replica data
rm -r /tmp/springtail/table
rm -rf /tmp/repl_logs/*
rm -rf /tmp/xact_logs/*
redis-cli flushdb
rm -f /tmp/xid_mgr.log /tmp/write_cache.log /tmp/pg_log_mgr.log /tmp/gc.log

# install the FDW extension into postgres
./install_fdw.sh ${BUILD_DIR}

# create the data tables for testing
${PSQL_CMD} < create.sql

# start the data replication
${PSQL_CMD} < start_replication.sql

# start the XID mgr
echo Start XID Mgr...
XID_DAEMON="${BUILD_DIR}/src/xid_mgr/xid_mgr_daemon"
SPRINGTAIL_PROPERTIES="logging.log_path=/tmp/xid_mgr.log" ${XID_DAEMON} -x 10 --daemon
sleep 1

# start the SysTblMgr
echo Start SysTblMgr...
SYS_TBL_DAEMON="${BUILD_DIR}/src/sys_tbl_mgr/sys_tbl_mgr_daemon"
SPRINGTAIL_PROPERTIES="logging.log_path=/tmp/sys_tbl_mgr.log" ${SYS_TBL_DAEMON} --daemon
sleep 1

# copy the initial snapshot of the table data
${BUILD_DIR}/src/pg_fdw/copy_schema -d springtail -u springtail -s public

# start the write cache
echo Start Write Cache...
WRITE_CACHE_DAEMON="${BUILD_DIR}/src/write_cache/write_cache_daemon"
SPRINGTAIL_PROPERTIES="logging.log_path=/tmp/write_cache.log" ${WRITE_CACHE_DAEMON} --daemon
sleep 1

# start the pg log mgr
# echo Start PG Log Mgr...
rm -rdf /tmp/xact_logs /tmp/repl_logs
mkdir /tmp/xact_logs /tmp/repl_logs
PG_LOG_DAEMON="${BUILD_DIR}/src/pg_log_mgr/pg_log_mgr_daemon"
SPRINGTAIL_PROPERTIES="logging.log_path=/tmp/pg_log_mgr.log" ${PG_LOG_DAEMON} --daemon -d springtail -P springtail -b springtail_pub -s springtail_slot -x /tmp/xact_logs -r /tmp/repl_logs
sleep 1

# start the garbage collector
# echo Start Garbage Collector...
# GC_DAEMON="${BUILD_DIR}/src/garbage_collector/gc_daemon"
# SPRINGTAIL_PROPERTIES="logging.log_path=/tmp/gc.log" ${GC_DAEMON} --daemon
# sleep 1

# set up the replica database
# note: using PSQL_CMD here as the replica DB does not exist yet
${PSQL_CMD} < setup_replica_db.sql

# verify the snapshot by running a query against the FDW
echo "SELECT count(*) FROM test_data; SELECT count(*) FROM test_data WHERE a = 1 AND b = 'a';" | ${PSQL_CMD_REPLICA}

# now we are ready to modify the data
# ${PSQL_CMD} < test.sql

# verify the data
# echo "SELECT count(*) FROM test_data; SELECT * FROM test_data;" | ${PSQL_CMD_REPLICA}
