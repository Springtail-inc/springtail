## test
CREATE TABLE IF NOT EXISTS test_null_quals (
    id SERIAL PRIMARY KEY,
    a INTEGER,
    b INTEGER
);

CREATE INDEX test_null_quals_a_idx ON test_null_quals (a);
INSERT INTO test_null_quals (a, b) VALUES (1, 1), (NULL, NULL), (3, 3), (NULL, NULL);

## verify
SELECT * FROM test_null_quals ORDER BY id;
SELECT id FROM test_null_quals WHERE a = 1;
SELECT id FROM test_null_quals WHERE a IS NULL;
SELECT id FROM test_null_quals WHERE a IS NOT NULL;
SELECT id FROM test_null_quals WHERE b IS NULL;
SELECT id FROM test_null_quals WHERE b IS NOT NULL;

## cleanup
DROP TABLE IF EXISTS test_null_quals;
DROP INDEX IF EXISTS test_null_quals_a_idx;