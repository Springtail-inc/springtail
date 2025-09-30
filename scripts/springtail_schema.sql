-- Helper script to grant full access to the __pg_springtail_triggers
-- Variables: username
-- Usage: psql -f scripts/springtail_schema.sql -v username=springtail_user -d your_database
\set ON_ERROR_STOP on
-- Expect: -v username=some_user (no quotes)
\if :{?username}
\echo Granting full access on schema __pg_springtail_triggers to role :username
\else
\quit 1
\endif

-- 0) create the schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS __pg_springtail_triggers;

-- 1) The schema itself
GRANT USAGE, CREATE ON SCHEMA __pg_springtail_triggers TO :username;

-- 2) Existing objects in the schema
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA __pg_springtail_triggers TO :username;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA __pg_springtail_triggers TO :username;
GRANT ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA __pg_springtail_triggers TO :username;

-- 3) Future objects created in this schema
ALTER DEFAULT PRIVILEGES IN SCHEMA __pg_springtail_triggers
    GRANT ALL PRIVILEGES ON TABLES TO :username;
ALTER DEFAULT PRIVILEGES IN SCHEMA __pg_springtail_triggers
    GRANT ALL PRIVILEGES ON SEQUENCES TO :username;
ALTER DEFAULT PRIVILEGES IN SCHEMA __pg_springtail_triggers
    GRANT EXECUTE ON ROUTINES TO :username;
ALTER DEFAULT PRIVILEGES IN SCHEMA __pg_springtail_triggers
    GRANT USAGE ON TYPES TO :username;