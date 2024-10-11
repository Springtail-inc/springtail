## setup
-- Create a table for the test
CREATE TABLE IF NOT EXISTS test3 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Insert initial data
INSERT INTO test3 (value) VALUES ('delete_me');

## test
-- Delete the inserted data
DELETE FROM test3 WHERE value = 'delete_me';

## verify
-- Expected: []
-- Verify the data has been deleted
SELECT * FROM test3 WHERE value = 'delete_me';

## cleanup
-- Clean up by removing the test table
DROP TABLE IF EXISTS test3;