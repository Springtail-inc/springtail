## setup
-- Create a table for the test
CREATE TABLE IF NOT EXISTS test4 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Insert data for selection test
INSERT INTO test4 (value) VALUES ('select_value_1');
INSERT INTO test4 (value) VALUES ('select_value_2');

## test
-- Select data from the table
SELECT * FROM test4 ORDER BY id;

## verify
-- Verify that the selected data matches expectations
SELECT * FROM test4 WHERE value = 'select_value_1';

## cleanup
-- Clean up by removing the test table
DROP TABLE IF EXISTS test4;
