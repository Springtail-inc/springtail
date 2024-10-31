## setup
-- a pre-created table available to the DDL tests
CREATE TABLE IF NOT EXISTS ddl_test (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- add a row to ensure data is retrievable
INSERT INTO ddl_test (value) VALUES ('test_value');

## cleanup
-- drop the table after tests have run
DROP TABLE IF EXISTS ddl_test;
