## test
CREATE TABLE IF NOT EXISTS table_without_notnull_constraint (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(100)
);

ALTER TABLE table_without_notnull_constraint ALTER COLUMN username SET NOT NULL;
ALTER TABLE table_without_notnull_constraint ALTER COLUMN email SET NOT NULL;

## verify
### schema_check public table_without_notnull_constraint 15

## cleanup
DROP TABLE IF EXISTS table_without_notnull_constraint;

