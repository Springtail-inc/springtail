## test
CREATE TABLE IF NOT EXISTS test1 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

INSERT INTO test1 (value) VALUES ('test_value_1');
INSERT INTO test1 (value) VALUES ('test_value_2');

## verify
SELECT * FROM test1 ORDER BY id;

## cleanup
DROP TABLE IF EXISTS test1;
