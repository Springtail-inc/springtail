## test
CREATE TABLE IF NOT EXISTS test1 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

INSERT INTO test1 (value) VALUES ('test_value_1');
INSERT INTO test1 (value) VALUES ('test_value_2');

## verify
PREPARE prepare_test AS SELECT * FROM test1 ORDER BY id;
PREPARE prepare_test_param (text) AS SELECT * FROM test1 where value = $1 ORDER BY id;
EXECUTE prepare_test;
EXECUTE prepare_test;
EXECUTE prepare_test;

EXECUTE prepare_test_param('test_value_1');
EXECUTE prepare_test_param('test_value_1');
EXECUTE prepare_test_param('test_value_2');
EXECUTE prepare_test_param('test_value_2');

## cleanup
DROP TABLE IF EXISTS test1;
