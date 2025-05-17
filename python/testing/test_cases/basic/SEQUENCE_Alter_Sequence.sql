## test
-- Step 1: Create a sequence and table with a sequence-based column
CREATE SEQUENCE IF NOT EXISTS test_seq START 1 INCREMENT 1;

CREATE TABLE IF NOT EXISTS table_with_test_seq (
    id INT NOT NULL DEFAULT nextval('test_seq') PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Step 2: Insert a row
INSERT INTO table_with_test_seq (name) VALUES ('First Entry');

-- Step 3: Modify the sequence to increase its value
ALTER SEQUENCE test_seq RESTART WITH 100;

-- Step 4: Insert another row
INSERT INTO table_with_test_seq (name) VALUES ('Second Entry');

## verify
-- Verify the table data
SELECT * FROM table_with_test_seq ORDER BY id;

## cleanup
DROP TABLE IF EXISTS table_with_test_seq;
DROP SEQUENCE IF EXISTS test_seq;

