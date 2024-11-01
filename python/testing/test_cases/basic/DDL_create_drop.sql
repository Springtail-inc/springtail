## test
CREATE TABLE IF NOT EXISTS ddl_create_drop (id SERIAL PRIMARY KEY, value TEXT);
DROP TABLE ddl_create_drop;

## verify
SELECT table_name FROM information_schema.tables WHERE table_name = 'ddl_create_drop';

## cleanup
SELECT 1;
