-- Initialization testing
-- This script will be passed to springtail.py using the -s option

-- Create tables and insert data in the primary database during initialization
CREATE TABLE IF NOT EXISTS test_init (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- Ensure the table is owned by springtail user
ALTER TABLE test_init OWNER TO springtail;

INSERT INTO test_init (value) VALUES ('initial_value_1');
INSERT INTO test_init (value) VALUES ('initial_value_2');
INSERT INTO test_init (value) VALUES ('initial_value_3');
INSERT INTO test_init (value) VALUES ('initial_value_4');