-- setup
CREATE TABLE IF NOT EXISTS transaction_test_update (
    id SERIAL PRIMARY KEY,
    value TEXT
);

INSERT INTO transaction_test_update (value) VALUES ('original_value1');
INSERT INTO transaction_test_update (value) VALUES ('original_value2');
INSERT INTO transaction_test_update (value) VALUES ('original_value3');

-- test
BEGIN;

-- Update a row successfully
UPDATE transaction_test_update SET value = 'updated_value1' WHERE id = 1;

-- Start a sub-transaction to update another row and abort
SAVEPOINT sp2;
UPDATE transaction_test_update SET value = 'updated_value2_abort' WHERE id = 2;
ROLLBACK TO SAVEPOINT sp2;

-- Final update that should succeed after the aborted sub-transaction
UPDATE transaction_test_update SET value = 'updated_value3' WHERE id = 3;

COMMIT;

-- verify
SELECT * FROM transaction_test_update ORDER BY id;

-- cleanup
DROP TABLE IF EXISTS transaction_test_update;
