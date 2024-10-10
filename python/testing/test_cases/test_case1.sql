-- -- PHASE 1 -- table creation
-- CREATE TABLE test1 (colA text PRIMARY KEY, colB bigint);

-- -- PHASE 2 -- data modification
-- INSERT INTO test1 (colA, colB) VALUES ('a', 1), ('b', 2), ('c', 3);

-- -- PHASE 3 -- data verification
-- SELECT * FROM test1 ORDER BY colA;

-- -- PHASE 4 -- data cleanup
-- DROP TABLE test1;

## setup
-- Create a table for the test
CREATE TABLE IF NOT EXISTS test1 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

## test
-- Insert test data
INSERT INTO test1 (value) VALUES ('test_value_1');
INSERT INTO test1 (value) VALUES ('test_value_2');

## verify
-- Verify the inserted data
SELECT * FROM test1 ORDER BY id;

## cleanup
-- Clean up by removing the test table
DROP TABLE IF EXISTS test1;
