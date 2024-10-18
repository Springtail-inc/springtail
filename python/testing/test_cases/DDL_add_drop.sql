-- Test Case 3: Add and Drop Column

-- Create a table for testing column mutations.
CREATE TABLE IF NOT EXISTS ddl_test_column (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Add an extra column.
ALTER TABLE ddl_test_column ADD COLUMN extra_column TEXT;

-- Insert data into the table with the new column.
INSERT INTO ddl_test_column (value, extra_column) VALUES ('value1', 'extra1');

-- Query the table to ensure the extra column is present.
SELECT * FROM ddl_test_column;

-- Drop the extra column.
ALTER TABLE ddl_test_column DROP COLUMN extra_column;

-- Query the table again to ensure the column is removed.
SELECT * FROM ddl_test_column;

-- Drop the table to clean up.
DROP TABLE IF EXISTS ddl_test_column;
