## test
CREATE TABLE IF NOT EXISTS ddl_test_not_null (
      id SERIAL PRIMARY KEY,
      value TEXT
);

CREATE INDEX idx_test_not_null ON ddl_test_not_null(id, value);

INSERT into ddl_test_not_null(id,value) values (37,42);

ALTER TABLE ddl_test_not_null DROP COLUMN value;

INSERT into ddl_test_not_null(id) values (38);

## verify
### schema_check public ddl_test_not_null
SELECT * FROM ddl_test_not_null ORDER BY id;

## cleanup
DROP INDEX IF EXISTS idx_test_not_null;
DROP TABLE IF EXISTS ddl_test_not_null;
