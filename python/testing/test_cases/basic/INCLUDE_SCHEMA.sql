## test

-- Change from wildcart to list
-- Schemas that are not in the list should be removed
CREATE SCHEMA IF NOT EXISTS unlisted_schema;
CREATE TABLE unlisted_schema.test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SELECT pg_sleep(2);
### schema_exists springtail unlisted_schema true

-- 1. transition from ['*'] to explicit list, schemas do not exist ye
### set_include_schema springtail '["public", "test1", "test2", "test3"]'
### schema_exists springtail public true
### schema_exists springtail unlisted_schema false
### schema_exists springtail test1 false
### schema_exists springtail test2 false
### schema_exists springtail test3 false

-- Add schema tests
-- 1. add schema that does not exist and verify it accessible through FDW
CREATE SCHEMA IF NOT EXISTS test1;
SELECT pg_sleep(2);
### schema_exists springtail test1 true

-- 2. add schema that is not in the schema list yet
CREATE SCHEMA IF NOT EXISTS test4;
SELECT pg_sleep(2);
### schema_exists springtail test4 false

-- 3. add schema that already exists to include list, it won't get replicated till there are some tables in it
### set_include_schema springtail '["public", "test1", "test2", "test3", "test4"]'
-- NOTE: schema "test4" won't get replicated unless it has some tables
### schema_exists springtail test4 false

-- 4. add table to an empty schema and verify the schema is replicated now
CREATE TABLE test4.test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SELECT pg_sleep(2);
### schema_exists springtail test4 true

-- 5. create a schema with a table, then add it to the list and verify it is replicated
CREATE SCHEMA IF NOT EXISTS test5;
CREATE TABLE test5.test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SELECT pg_sleep(2);
### schema_exists springtail test5 false
### set_include_schema springtail '["public", "test1", "test2", "test3", "test4", "test5"]'
### schema_exists springtail test5 true

-- Remove schema tests

-- 1. remove schema that exists and is in the list
DROP SCHEMA IF EXISTS test5 CASCADE;
SELECT pg_sleep(2);
### schema_exists springtail test5 false

-- 2. remove schema from the list
### set_include_schema springtail '["public", "test1", "test2", "test3", "test4"]'

-- 3. remove schema from the list before removing it from the database
### set_include_schema springtail '["public", "test1", "test2", "test3"]'
### schema_exists springtail test4 false

-- 4. remove schema from the database
DROP SCHEMA IF EXISTS test4 CASCADE;

-- Change from list to wildcard
-- Schemas that were not in the list should be added
### set_include_schema springtail '["*"]'
-- NOTE: won't get replicated unless it has some tables
### schema_exists springtail unlisted_schema true

## verify

### schema_exists springtail public true
### schema_exists springtail unlisted_schema true
### schema_exists springtail test1 true
### schema_exists springtail test2 false
### schema_exists springtail test3 false
### schema_exists springtail test4 false
### schema_exists springtail test5 false

## cleanup
