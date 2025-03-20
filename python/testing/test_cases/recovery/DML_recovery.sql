## metadata
### sync_timeout 20

## test
CREATE TABLE IF NOT EXISTS test1 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

INSERT INTO test1 (value) VALUES ('test_value_1');
INSERT INTO test1 (value) VALUES ('test_value_2');
INSERT INTO test1 (value) VALUES ('test_value_3');
INSERT INTO test1 (value) VALUES ('test_value_4');

-- revert to before the INSERTs, recover from there
### sync
### force_recovery 4

## verify
SELECT * FROM test1 ORDER BY id;

## cleanup
DROP TABLE IF EXISTS test1;
