CREATE PUBLICATION springtail_pub FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('springtail_slot', 'pgoutput');
\i triggers.sql
