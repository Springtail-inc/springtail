-- DISABLED_TEST
-- ## test
-- CREATE TABLE IF NOT EXISTS table_with_unique_constraint (
--     id SERIAL PRIMARY KEY,
--     username VARCHAR(50) NOT NULL,
--     email VARCHAR(100) NOT NULL,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- ALTER TABLE table_with_unique_constraint ADD CONSTRAINT unique_username UNIQUE (username);
-- ALTER TABLE table_with_unique_constraint ADD CONSTRAINT unique_email UNIQUE (email);

-- ## verify
-- ### schema_check public table_with_unique_constraint

-- ## cleanup
-- DROP TABLE IF EXISTS table_with_unique_constraint;

