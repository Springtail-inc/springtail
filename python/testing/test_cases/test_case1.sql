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
-- Expected: [(1, 'test_value_1'), (2, 'test_value_2')]
SELECT * FROM test1 ORDER BY id;

## cleanup
-- Clean up by removing the test table
DROP TABLE IF EXISTS test1;
