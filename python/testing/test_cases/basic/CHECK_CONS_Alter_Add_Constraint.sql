## test
CREATE TABLE IF NOT EXISTS table_with_check_constraint (
    id SERIAL PRIMARY KEY,
    age INT NOT NULL,
    salary NUMERIC(10, 2)
);

ALTER TABLE table_with_check_constraint ADD CONSTRAINT check_age CHECK (age >= 18);
ALTER TABLE table_with_check_constraint ADD CONSTRAINT check_salary CHECK (salary >= 0);

## verify
### schema_check public table_with_check_constraint

## cleanup
DROP TABLE IF EXISTS table_with_check_constraint;

