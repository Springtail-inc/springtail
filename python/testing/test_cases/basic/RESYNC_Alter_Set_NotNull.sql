## test
-- Create a table with a nullable column.
CREATE TABLE IF NOT EXISTS ddl_test_force_resync (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Set the column to NOT NULL, forcing a re-sync.
ALTER TABLE ddl_test_force_resync
ALTER COLUMN value SET NOT NULL;

## verify
-- Verify that the column has the NOT NULL constraint.
SELECT * FROM ddl_test_force_resync ORDER BY id;
### schema_check public ddl_test_force_resync

## cleanup
-- Drop the table after the test.
DROP TABLE IF EXISTS ddl_test_force_resync;
