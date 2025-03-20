## test
-- Create a table
CREATE TABLE IF NOT EXISTS test1 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

INSERT INTO test1 (value) VALUES ('test_value_1');

-- Perform a transaction that is aborted
BEGIN;
INSERT INTO test1 (value) VALUES ('test_value_2');
INSERT INTO test1 (value) VALUES ('test_value_3');
ROLLBACK;

## verify
SELECT * FROM test1 ORDER BY id;

## cleanup
DROP TABLE IF EXISTS test1;
