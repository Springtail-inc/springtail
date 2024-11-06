## test
-- Create a table with a TEXT column.
CREATE TABLE IF NOT EXISTS ddl_test_alter_column (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- Alter the column's data type from TEXT to VARCHAR(255).
INSERT INTO ddl_test_alter_column (data) VALUES ('test_data');
ALTER TABLE ddl_test_alter_column ALTER COLUMN data TYPE VARCHAR(255);

## verify
-- Verify that the column type has changed to VARCHAR(255).
SELECT * FROM ddl_test_alter_column ORDER BY id;
### schema_check public ddl_test_alter_column

## cleanup
-- Drop the table after the test.
DROP TABLE IF EXISTS ddl_test_alter_column;
