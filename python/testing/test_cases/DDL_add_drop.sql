## setup
-- Create a table for testing column mutations.
CREATE TABLE IF NOT EXISTS ddl_test_column (
    id SERIAL PRIMARY KEY,
    value TEXT
);

## test
-- Add an extra column.
ALTER TABLE ddl_test_column ADD COLUMN extra_column TEXT; SELECT column_name FROM information_schema.columns WHERE table_name = 'ddl_test_column' AND column_name = 'extra_column';

-- Insert data into the table with the new column.
INSERT INTO ddl_test_column (value, extra_column) VALUES ('value1', 'extra1');

-- Drop the extra column.
ALTER TABLE ddl_test_column DROP COLUMN extra_column;

## verify
-- Verify the column no longer exists.
SELECT column_name FROM information_schema.columns 
WHERE table_name = 'ddl_test_column' AND column_name = 'extra_column';

## cleanup
-- Cleanup: Drop the table.
DROP TABLE IF EXISTS ddl_test_column;
