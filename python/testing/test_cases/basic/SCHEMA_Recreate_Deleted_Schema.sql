## test
CREATE SCHEMA IF NOT EXISTS test_to_be_deleted_schema;

CREATE TABLE IF NOT EXISTS test_to_be_deleted_schema.test_to_be_deleted_schema_table_1 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

DROP SCHEMA IF EXISTS test_to_be_deleted_schema CASCADE;

CREATE SCHEMA IF NOT EXISTS test_to_be_deleted_schema;

CREATE TABLE IF NOT EXISTS test_to_be_deleted_schema.test_to_be_deleted_schema_table_new (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

## verify
### schema_check test_to_be_deleted_schema test_to_be_deleted_schema_table_new

SELECT * FROM test_to_be_deleted_schema.test_to_be_deleted_schema_table_new;

## cleanup
DROP SCHEMA IF EXISTS test_to_be_deleted_schema CASCADE;