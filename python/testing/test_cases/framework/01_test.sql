## metadata
### autocommit false

## test
INSERT INTO test (table_id, name, "offset") VALUES (10000, 'foo', 12345);

## verify
SELECT count(*) FROM test;
SELECT * FROM test WHERE table_id = 10000;
