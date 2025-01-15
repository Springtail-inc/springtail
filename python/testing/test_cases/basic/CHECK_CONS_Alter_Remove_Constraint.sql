## test
CREATE TABLE IF NOT EXISTS table_with_check_constraint (
    id SERIAL PRIMARY KEY,
    age INT NOT NULL CHECK (age >= 18),
    salary NUMERIC(10, 2) CHECK (salary >= 0)
);

ALTER TABLE table_with_check_constraint DROP CONSTRAINT IF EXISTS check_age;
ALTER TABLE table_with_check_constraint DROP CONSTRAINT IF EXISTS check_salary;

## verify
### schema_check public table_with_check_constraint

## cleanup
DROP TABLE IF EXISTS table_with_check_constraint;

