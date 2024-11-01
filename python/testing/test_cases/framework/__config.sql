## metadata
### autocommit true

## setup
-- create the test table
CREATE TABLE test (table_id INT, name TEXT, "offset" INT, PRIMARY KEY (table_id, name) );

-- load the CSV file into the test table
### load_csv test.csv test

-- create the test table
CREATE TABLE test2 (table_id INT, name TEXT, "offset" INT );

-- load the CSV file into the test table
### load_csv test.csv test2

## cleanup
DROP TABLE test;
