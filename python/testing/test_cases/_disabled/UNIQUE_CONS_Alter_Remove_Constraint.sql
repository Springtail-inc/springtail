-- DISABLED_TEST
## metadata
### disable_test

## test
CREATE TABLE IF NOT EXISTS table_with_unique_constraint (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE table_with_unique_constraint DROP CONSTRAINT IF EXISTS unique_username;
ALTER TABLE table_with_unique_constraint DROP CONSTRAINT IF EXISTS unique_email;

## verify
### schema_check public table_with_unique_constraint

## cleanup
DROP TABLE IF EXISTS table_with_unique_constraint;

