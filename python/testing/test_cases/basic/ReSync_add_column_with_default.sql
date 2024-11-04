## test
-- Drop the table if it already exists to avoid conflicts.
DROP TABLE IF EXISTS ddl_test_add_column_default;

-- Create a table without the 'status' column.
CREATE TABLE ddl_test_add_column_default (
    id SERIAL PRIMARY KEY
);

-- Add a new column with a default value.
INSERT INTO ddl_test_add_column_default DEFAULT VALUES;
ALTER TABLE ddl_test_add_column_default ADD COLUMN status TEXT DEFAULT 'active';

-- Insert a row to verify the default value is applied.
INSERT INTO ddl_test_add_column_default DEFAULT VALUES;

## verify
-- Query the row to ensure the default value was applied correctly.
SELECT status 
FROM ddl_test_add_column_default
WHERE id = 1;

## cleanup
-- Drop the table after the test.
DROP TABLE IF EXISTS ddl_test_add_column_default;
