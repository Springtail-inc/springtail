## setup
-- Create a table with a TEXT column.
CREATE TABLE IF NOT EXISTS ddl_test_alter_column (
    id SERIAL PRIMARY KEY,
    data TEXT
);

## test
-- Alter the column's data type from TEXT to VARCHAR(255).
ALTER TABLE ddl_test_alter_column 
ALTER COLUMN data TYPE VARCHAR(255);

## verify
-- Verify that the column type has changed to VARCHAR(255).
SELECT data_type 
FROM information_schema.columns 
WHERE table_name = 'ddl_test_alter_column' AND column_name = 'data';

## cleanup
-- Drop the table after the test.
DROP TABLE IF EXISTS ddl_test_alter_column;
