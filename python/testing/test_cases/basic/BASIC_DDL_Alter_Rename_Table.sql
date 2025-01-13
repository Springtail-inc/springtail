## test
-- Create a table to rename.
CREATE TABLE IF NOT EXISTS ddl_test_rename (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Insert data to ensure the rename works with existing rows.
INSERT INTO ddl_test_rename (value) VALUES ('test_value');

-- Rename the table.
ALTER TABLE ddl_test_rename RENAME TO ddl_test_renamed;

## verify
-- Query the renamed table to ensure the change is applied.
SELECT * FROM ddl_test_renamed;

## cleanup
-- Drop the renamed table to clean up.
DROP TABLE IF EXISTS ddl_test_renamed;
