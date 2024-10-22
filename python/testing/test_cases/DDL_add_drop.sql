## setup
CREATE TABLE IF NOT EXISTS ddl_test_column (id SERIAL PRIMARY KEY, value TEXT);

## test
ALTER TABLE ddl_test_column ADD COLUMN extra_column TEXT;
SELECT column_name FROM information_schema.columns WHERE table_name = 'ddl_test_column' AND column_name = 'extra_column';
INSERT INTO ddl_test_column (value, extra_column) VALUES ('value1', 'extra1');
ALTER TABLE ddl_test_column DROP COLUMN extra_column;

## verify
SELECT column_name FROM information_schema.columns WHERE table_name = 'ddl_test_column' AND column_name = 'extra_column';

## cleanup
DROP TABLE IF EXISTS ddl_test_column;