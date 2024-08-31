-- drop the logical replica database
DROP DATABASE IF EXISTS replica_springtail;

-- remove the FDW
DROP SERVER IF EXISTS springtail_fdw_server;
DROP EXTENSION IF EXISTS springtail_fdw;

-- clear the replication triggers
DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_drops;
DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_ddl;

DROP FUNCTION IF EXISTS springtail_add_replica_identity_full;
DROP FUNCTION IF EXISTS springtail_event_trigger_for_drops;
DROP FUNCTION IF EXISTS springtail_event_trigger_for_ddl;

-- remove the replication setup
SELECT pg_drop_replication_slot('springtail_slot');
DROP PUBLICATION IF EXISTS springtail_pub;

-- remove the test data
DROP TABLE test_data;
DROP TABLE test_data2;
DROP TABLE test_data3;
DROP TABLE test_data4;
