## test
CREATE TABLE IF NOT EXISTS table_with_notnull_constraint_alter_add (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE table_with_notnull_constraint_alter_add ALTER COLUMN username DROP NOT NULL;
ALTER TABLE table_with_notnull_constraint_alter_add ALTER COLUMN email DROP NOT NULL;

-- Insert
INSERT INTO table_with_notnull_constraint_alter_add (username, email)
VALUES ('user2', 'user2@example.com');

-- Update
UPDATE table_with_notnull_constraint_alter_add
SET email = 'newuser2@example.com'
WHERE username = 'user2';

-- Delete
DELETE FROM table_with_notnull_constraint_alter_add
WHERE username = 'user2';

## verify
### schema_check public table_with_notnull_constraint_alter_add
SELECT * FROM table_with_notnull_constraint_alter_add;

## cleanup
DROP TABLE IF EXISTS table_with_notnull_constraint_alter_add;

