## setup
CREATE TABLE IF NOT EXISTS test2 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

## test
INSERT INTO test2 (value) VALUES ('initial_value');


UPDATE test2 SET value = 'updated_value' WHERE id = 1;

## verify
SELECT * FROM test2 WHERE id = 1;

## cleanup
DROP TABLE IF EXISTS test2;