-- Initialization testing
-- This script will be passed to springtail.py using the -s option

-- btree redesign phase 10: pg_trgm must exist before the daemon's one-time
-- PgCopyTable::init_pg_extn_registry() startup-sync call (system.json.settings'
-- extension_config already lists pg_trgm for this db) -- that call queries pg_opclass/pg_proc
-- for whatever extension_config names, filtered by "the extension is actually installed"
-- (pg_depend), so without this CREATE EXTENSION here (run before start_daemons(), per
-- springtail.py's start() ordering) the daemon would dlopen pg_trgm.so but register zero
-- opclass functions for it, and any later CREATE INDEX ... USING gist(...) using
-- gist_trgm_ops would fail at build time with no registered GIST_* support functions.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create tables and insert data in the primary database during initialization
CREATE TABLE IF NOT EXISTS test_init (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Ensure the table is owned by springtail user
ALTER TABLE test_init OWNER TO springtail;

INSERT INTO test_init (value) VALUES ('initial_value_1');
INSERT INTO test_init (value) VALUES ('initial_value_2');
INSERT INTO test_init (value) VALUES ('initial_value_3');
INSERT INTO test_init (value) VALUES ('initial_value_4');