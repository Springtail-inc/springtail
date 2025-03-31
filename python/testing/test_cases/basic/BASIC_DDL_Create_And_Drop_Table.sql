## test
CREATE TABLE IF NOT EXISTS ddl_create_drop (id SERIAL PRIMARY KEY, value TEXT);
DROP TABLE ddl_create_drop;

## verify
### schema_check public ddl_create_drop
### table_exists public ddl_create_drop false

## cleanup
SELECT 1;
