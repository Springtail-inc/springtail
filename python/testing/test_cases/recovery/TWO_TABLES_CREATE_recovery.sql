## test
CREATE TABLE IF NOT EXISTS test1 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

### recovery_point

INSERT INTO test1 (value) VALUES ('test_value_1');
INSERT INTO test1 (value) VALUES ('test_value_2');
INSERT INTO test1 (value) VALUES ('test_value_3');
INSERT INTO test1 (value) VALUES ('test_value_4');

CREATE TABLE IF NOT EXISTS test2 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- revert to before the INSERTs, recover from there
### force_recovery

## verify
SELECT * FROM test1 ORDER BY id;

## cleanup
DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;
