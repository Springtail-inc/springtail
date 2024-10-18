-- Test Case 1: Create and Drop Table

-- Create a simple table with a primary key and text column.
CREATE TABLE IF NOT EXISTS ddl_test_create (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Query the table to ensure it's created correctly.
SELECT * FROM ddl_test_create;

-- Drop the table to clean up.
DROP TABLE IF EXISTS ddl_test_create;
