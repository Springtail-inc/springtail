## test
CREATE SCHEMA IF NOT EXISTS test_schema;
CREATE SCHEMA IF NOT EXISTS schema_not_included;

CREATE TYPE test_schema.my_type AS ENUM ('a', 'b', 'c');
CREATE TYPE schema_not_included.my_type AS ENUM ('a', 'b', 'c');

CREATE TABLE test_schema.ddl_create(id SERIAL PRIMARY KEY, value test_schema.my_type);
CREATE TABLE IF NOT EXISTS test_schema.ddl_create_bad_type (id SERIAL PRIMARY KEY, value schema_not_included.my_type);

## verify
### table_exists test_schema ddl_create true
### table_exists test_schema ddl_create_bad_type false

## cleanup
DROP SCHEMA IF EXISTS test_schema CASCADE;
DROP SCHEMA IF EXISTS schema_not_included CASCADE;
