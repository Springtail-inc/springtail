## test
-- Create a table for testing column renaming.
CREATE TABLE IF NOT EXISTS ddl_test_rename_column (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Insert data into the table using the original column.
INSERT INTO ddl_test_rename_column (value) VALUES ('original_value');

-- Rename the column from 'value' to 'renamed_value'.
ALTER TABLE ddl_test_rename_column RENAME COLUMN value TO renamed_value;

## verify
-- Query the table to verify the column rename.
SELECT * FROM ddl_test_rename_column;

## cleanup
-- Drop the table to clean up.
DROP TABLE IF EXISTS ddl_test_rename_column;
