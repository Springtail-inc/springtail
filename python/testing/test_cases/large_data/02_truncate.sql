## test
-- Create a table with a large dataset.
CREATE TABLE IF NOT EXISTS truncate_test (
    id SERIAL PRIMARY KEY,
    sample_data TEXT
);

-- Insert a sample dataset. Adjust the number of rows as needed.
INSERT INTO truncate_test (sample_data)
SELECT md5(random()::text)
FROM generate_series(1, 1000000);  -- Insert 1M rows for testing

-- Perform a TRUNCATE operation to clear all data.
TRUNCATE TABLE truncate_test;

## verify
-- Verify that the row count is zero in both primary and replica databases.
SELECT COUNT(*) AS row_count FROM truncate_test;

## cleanup
-- Drop the table after the test.
DROP TABLE IF EXISTS truncate_test;
