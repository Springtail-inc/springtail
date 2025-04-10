## test

BEGIN;
    CREATE TABLE IF NOT EXISTS ddl_test_same_txn (
        id SERIAL PRIMARY KEY,
        value TEXT);
    CREATE INDEX idx_test_same_txn ON ddl_test_same_txn(id, value);
    INSERT into ddl_test_same_txn(id,value) values (37,42);
COMMIT;

BEGIN;
    CREATE INDEX idx2 on ddl_test_same_txn(id);
    CREATE INDEX idx3 on ddl_test_same_txn(id);
    CREATE INDEX idx4 on ddl_test_same_txn(id);
    CREATE INDEX idx5 on ddl_test_same_txn(id);
    DROP INDEX idx4;
    DROP INDEX idx3;
    INSERT into ddl_test_same_txn(id) values (395);
    INSERT into ddl_test_same_txn(id) values (397);
    INSERT into ddl_test_same_txn(id) values (398);
COMMIT;

## verify
### schema_check public ddl_test_same_txn
SELECT * FROM ddl_test_same_txn ORDER BY id;

## cleanup
DROP INDEX idx2;
DROP INDEX idx5;
DROP TABLE IF EXISTS ddl_test_same_txn;
