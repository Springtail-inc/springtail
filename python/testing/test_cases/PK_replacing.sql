## setup
-- Create a table with an initial primary key.
CREATE TABLE IF NOT EXISTS test_replace_pkey (
    A INT,
    B INT,
    value TEXT,
    PRIMARY KEY (A)
);

## test
-- Drop and replace the primary key.
ALTER TABLE test_replace_pkey DROP CONSTRAINT test_replace_pkey_pkey;
ALTER TABLE test_replace_pkey ADD PRIMARY KEY (B);

-- Insert data to ensure the new primary key works.
INSERT INTO test_replace_pkey (A, B, value) VALUES (1, 2, 'test_value');

## verify
-- Verify that the new primary key is on column B.
SELECT column_name
FROM information_schema.key_column_usage
WHERE table_name = 'test_replace_pkey';

## cleanup
-- Cleanup
DROP TABLE IF EXISTS test_replace_pkey;
