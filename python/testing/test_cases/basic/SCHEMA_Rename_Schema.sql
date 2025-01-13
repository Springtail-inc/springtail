## test_disabled
CREATE SCHEMA IF NOT EXISTS test_schema_1;

CREATE TABLE IF NOT EXISTS test_schema_1.test_table_in_schema_1 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO test_schema_1.test_table_in_schema_1 (name) VALUES ('Table in schema 1');

ALTER SCHEMA test_schema_1 RENAME TO test_schema_2;

INSERT INTO test_schema_2.test_table_in_schema_1 (name) VALUES ('Table in schema 1 after schema move');

## verify
### schema_check test_schema_2 test_table_in_schema_1

SELECT * FROM test_schema_2.test_table_in_schema_1;

## cleanup
DROP SCHEMA IF EXISTS test_schema_1 CASCADE;
DROP SCHEMA IF EXISTS test_schema_2 CASCADE;

