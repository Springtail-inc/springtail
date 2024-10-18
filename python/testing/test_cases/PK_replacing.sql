-- Test Case 3: Replacing a Primary Key Column
CREATE TABLE IF NOT EXISTS test_replace_pkey (
    A INT,
    B INT,
    value TEXT,
    PRIMARY KEY (A)
);

ALTER TABLE test_replace_pkey DROP CONSTRAINT test_replace_pkey_pkey;
ALTER TABLE test_replace_pkey ADD PRIMARY KEY (B);

INSERT INTO test_replace_pkey (A, B, value) VALUES (1, 2, 'test_value');

-- Cleanup
DROP TABLE IF EXISTS test_replace_pkey;
