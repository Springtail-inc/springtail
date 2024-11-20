##test
-- setup
CREATE TABLE IF NOT EXISTS transaction_test_delete (
    id SERIAL PRIMARY KEY,
    value TEXT
);

INSERT INTO transaction_test_delete (value) VALUES ('delete_test1');
INSERT INTO transaction_test_delete (value) VALUES ('delete_test2');
INSERT INTO transaction_test_delete (value) VALUES ('delete_test3');

-- test
BEGIN;

-- Delete a row successfully
DELETE FROM transaction_test_delete WHERE id = 1;

-- Start a sub-transaction for another delete, then abort
SAVEPOINT sp3;
DELETE FROM transaction_test_delete WHERE id = 2;
ROLLBACK TO SAVEPOINT sp3;

-- Final delete that should succeed after the aborted sub-transaction
DELETE FROM transaction_test_delete WHERE id = 3;

COMMIT;
##verify
-- verify
SELECT * FROM transaction_test_delete ORDER BY id;
##cleanup
-- cleanup
DROP TABLE IF EXISTS transaction_test_delete;
