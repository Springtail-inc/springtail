## test
CREATE SCHEMA IF NOT EXISTS test_schema;
CREATE SCHEMA IF NOT EXISTS schema_not_included;

CREATE TABLE IF NOT EXISTS ddl_create (id SERIAL PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS test_schema.ddl_create (id SERIAL PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS test_schema.ddl_create_drop (id SERIAL PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS schema_not_included.ddl_create (id SERIAL PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS schema_not_included.ddl_create_drop (id SERIAL PRIMARY KEY, value TEXT);

CREATE INDEX idx_value ON schema_not_included.ddl_create(value);
DROP INDEX schema_not_included.idx_value;
ALTER TABLE schema_not_included.ddl_create DROP COLUMN value;

DROP TABLE test_schema.ddl_create_drop;
DROP TABLE schema_not_included.ddl_create_drop;

## verify
### table_exists public ddl_create true
### table_exists test_schema ddl_create true
### table_exists schema_not_included ddl_create false
### table_exists schema_not_included ddl_create_drop1 false
### table_exists test_schema ddl_create_drop false

## cleanup
SELECT 1;
