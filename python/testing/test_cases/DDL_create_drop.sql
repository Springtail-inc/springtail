## setup
-- Create a test table.
CREATE TABLE IF NOT EXISTS ddl_create_drop (
    id SERIAL PRIMARY KEY,
    value TEXT
);

## test
-- Drop the table to verify DDL operations.
DROP TABLE ddl_create_drop;

## verify
-- Ensure the table no longer exists.
SELECT * FROM information_schema.tables 
WHERE table_name = 'ddl_create_drop';

## cleanup
-- No cleanup needed since the table was dropped.
