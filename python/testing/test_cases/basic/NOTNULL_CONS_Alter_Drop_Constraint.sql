## test
CREATE TABLE IF NOT EXISTS table_with_notnull_constraint (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE table_with_notnull_constraint ALTER COLUMN username DROP NOT NULL;
ALTER TABLE table_with_notnull_constraint ALTER COLUMN email DROP NOT NULL;

## verify
### schema_check public table_with_notnull_constraint

## cleanup
DROP TABLE IF EXISTS table_with_notnull_constraint;

