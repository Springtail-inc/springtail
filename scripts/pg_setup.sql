CREATE USER springtail WITH PASSWORD 'springtail' SUPERUSER LOGIN;
CREATE DATABASE springtail OWNER springtail;
\c springtail
CREATE PUBLICATION springtail_pub FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('springtail_slot', 'pgoutput');
\i triggers.sql
