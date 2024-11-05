## test
-- Create a table with an initial primary key.
CREATE TABLE IF NOT EXISTS test_replace_pkey (
    A INT,
    B INT,
    value TEXT,
    PRIMARY KEY (A)
);

-- Insert initial data to ensure the table works with the original primary key.
INSERT INTO test_replace_pkey (A, B, value) VALUES (1, 2, 'original_value');

-- Drop and replace the primary key.
ALTER TABLE test_replace_pkey DROP CONSTRAINT test_replace_pkey_pkey;
ALTER TABLE test_replace_pkey ADD PRIMARY KEY (B);

-- Insert data to ensure the new primary key works.
INSERT INTO test_replace_pkey (A, B, value) VALUES (3, 4, 'new_value');

## verify
-- Verify that the new primary key is on column B.
-- SELECT column_name FROM information_schema.key_column_usage WHERE table_name = 'test_replace_pkey';
SELECT * from test_replace_pkey ORDER BY B;

## cleanup
-- Cleanup
DROP TABLE IF EXISTS test_replace_pkey;
