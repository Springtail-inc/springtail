CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create benchmark state table
DROP TABLE IF EXISTS benchmark_state;
CREATE TABLE benchmark_state (
    key TEXT PRIMARY KEY,
    state TEXT
);

DROP TABLE IF EXISTS benchmark_data;
CREATE TABLE benchmark_data (
    id INT PRIMARY KEY,
    value BYTEA
);

-- Insert sentinel value
INSERT INTO benchmark_state (key, state)
VALUES ('common_benchmark_setup', 'ready');
