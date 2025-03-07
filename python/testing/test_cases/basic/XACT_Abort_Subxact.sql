## test
-- Create a table
CREATE TABLE IF NOT EXISTS test1 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Perform a transaction with a subtransaction that is aborted.
BEGIN;
INSERT INTO test1 (value) VALUES ('test_value_1');
SAVEPOINT sp1;
INSERT INTO test1 (value) VALUES ('test_value_2');
ROLLBACK TO SAVEPOINT sp1;
INSERT INTO test1 (value) VALUES ('test_value_3');
COMMIT;

## verify
SELECT * FROM test1 ORDER BY id;

## cleanup
DROP TABLE IF EXISTS test1;
