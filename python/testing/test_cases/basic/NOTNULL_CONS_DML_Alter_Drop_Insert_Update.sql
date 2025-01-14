## test
CREATE TABLE IF NOT EXISTS table_with_notnull_constraint_alter_drop (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert
INSERT INTO table_with_notnull_constraint_alter_drop (username, email)
VALUES ('user2', 'user2@example.com');

-- Update
UPDATE table_with_notnull_constraint_alter_drop
SET email = 'newuser2@example.com'
WHERE username = 'user2';

-- Drop the NOT NULL constraint
ALTER TABLE table_with_notnull_constraint_alter_drop ALTER COLUMN username DROP NOT NULL;
ALTER TABLE table_with_notnull_constraint_alter_drop ALTER COLUMN email DROP NOT NULL;

-- Test insert with null value after dropping the constraint
INSERT INTO table_with_notnull_constraint_alter_drop (username, email)
VALUES (NULL, 'nulluser@example.com');

-- Test update with null value after dropping the constraint
UPDATE table_with_notnull_constraint_alter_drop
SET email = NULL
WHERE username = 'user2';

## verify
### schema_check public table_with_notnull_constraint_alter_drop
-- SELECT * FROM table_with_notnull_constraint_alter_drop;

## cleanup
DROP TABLE IF EXISTS table_with_notnull_constraint_alter_drop;

