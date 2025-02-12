## test
CREATE SCHEMA IF NOT EXISTS test_schema_1;
CREATE SCHEMA IF NOT EXISTS test_schema_2;

CREATE TABLE IF NOT EXISTS test_schema_1.test_table_in_schema_1 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS test_schema_2.test_table_in_schema_2 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO test_schema_1.test_table_in_schema_1 (name) VALUES ('Table in schema 1');
INSERT INTO test_schema_2.test_table_in_schema_2 (name) VALUES ('Table in schema 2');

## verify
### schema_check test_schema_1 test_table_in_schema_1
### schema_check test_schema_2 test_table_in_schema_2

## cleanup
DROP SCHEMA IF EXISTS test_schema_1 CASCADE;
DROP SCHEMA IF EXISTS test_schema_2 CASCADE;

