-- Test Case 2: Changing Primary Key Column Order
CREATE TABLE IF NOT EXISTS test_pkey_order (
    A INT,
    B INT,
    C INT,
    value TEXT,
    PRIMARY KEY (A, B, C)
);

ALTER TABLE test_pkey_order DROP CONSTRAINT test_pkey_order_pkey;
ALTER TABLE test_pkey_order ADD PRIMARY KEY (B, C, A);

INSERT INTO test_pkey_order (A, B, C, value) VALUES (1, 2, 3, 'test_value');

-- Cleanup
DROP TABLE IF EXISTS test_pkey_order;
