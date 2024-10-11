## setup
-- Create a table for the test
CREATE TABLE IF NOT EXISTS test2 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Insert initial data
INSERT INTO test2 (value) VALUES ('initial_value');

## test
-- Update the value in the table
UPDATE test2 SET value = 'updated_value' WHERE id = 1;

## verify
-- Expected: [(1, 'updated_value')]
-- Verify the updated data
SELECT * FROM test2 WHERE id = 1;

## cleanup
-- Clean up by removing the test table
DROP TABLE IF EXISTS test2;