## test
CREATE TABLE IF NOT EXISTS table_with_check_constraint (
    id SERIAL PRIMARY KEY,
    age INT NOT NULL CHECK (age >= 18),
    salary NUMERIC(10, 2) CHECK (salary >= 0)
);

## verify
### schema_check public table_with_check_constraint

## cleanup
DROP TABLE IF EXISTS table_with_check_constraint;

