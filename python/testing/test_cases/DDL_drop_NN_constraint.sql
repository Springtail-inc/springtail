-- Test Case 5: Drop NOT NULL Constraint

-- Create a table with a NOT NULL constraint on a column.
CREATE TABLE IF NOT EXISTS ddl_test_not_null (
    id SERIAL PRIMARY KEY,
    value TEXT NOT NULL
);

-- Drop the NOT NULL constraint from the 'value' column.
ALTER TABLE ddl_test_not_null ALTER COLUMN value DROP NOT NULL;

-- Insert a row with a NULL value to verify the constraint was dropped.
INSERT INTO ddl_test_not_null (value) VALUES (NULL);

-- Query the table to verify the change.
SELECT * FROM ddl_test_not_null;

-- Drop the table to clean up.
DROP TABLE IF EXISTS ddl_test_not_null;
