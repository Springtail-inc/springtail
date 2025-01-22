## test
CREATE TABLE IF NOT EXISTS table_with_notnull_constraint (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

## verify
### schema_check public table_with_notnull_constraint

## cleanup
DROP TABLE IF EXISTS table_with_notnull_constraint;

