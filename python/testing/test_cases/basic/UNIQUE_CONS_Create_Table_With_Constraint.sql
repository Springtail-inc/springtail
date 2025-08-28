-- DISABLED_TEST
## metadata
### disable_test

## test
CREATE TABLE IF NOT EXISTS table_with_unique_constraint (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP
);

## verify
### schema_check public table_with_unique_constraint

## cleanup
DROP TABLE IF EXISTS table_with_unique_constraint;
