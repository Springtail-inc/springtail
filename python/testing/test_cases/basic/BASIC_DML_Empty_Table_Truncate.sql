## test
CREATE TABLE IF NOT EXISTS empty_truncate_test (
    id SERIAL PRIMARY KEY,
    sample_data TEXT
);

-- Perform a TRUNCATE operation on the empty table
TRUNCATE TABLE empty_truncate_test;

## verify
### schema_check public empty_truncate_test
SELECT COUNT(*) AS row_count FROM empty_truncate_test;

## cleanup
-- Drop the table after the test.
DROP TABLE IF EXISTS empty_truncate_test;
