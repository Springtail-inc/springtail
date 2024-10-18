-- Test Case 2: Rename Table

-- Create a table to rename.
CREATE TABLE IF NOT EXISTS ddl_test_rename (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Rename the table.
ALTER TABLE ddl_test_rename RENAME TO ddl_test_renamed;

-- Query the renamed table to ensure the change is applied.
SELECT * FROM ddl_test_renamed;

-- Drop the renamed table to clean up.
DROP TABLE IF EXISTS ddl_test_renamed;
