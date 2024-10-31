## test
-- Create a table with a NOT NULL constraint on a column.
CREATE TABLE IF NOT EXISTS ddl_test_not_null (
    id SERIAL PRIMARY KEY,
    value TEXT NOT NULL
);

-- Insert data with non-NULL value to ensure the initial constraint.
INSERT INTO ddl_test_not_null (value) VALUES ('valid_value');

-- Drop the NOT NULL constraint from the 'value' column.
ALTER TABLE ddl_test_not_null ALTER COLUMN value DROP NOT NULL;

-- Insert a row with a NULL value to verify the constraint was dropped.
INSERT INTO ddl_test_not_null (value) VALUES (NULL);

## verify
-- Query the table to verify the change.
SELECT * FROM ddl_test_not_null;

## cleanup
-- Drop the table to clean up.
DROP TABLE IF EXISTS ddl_test_not_null;
