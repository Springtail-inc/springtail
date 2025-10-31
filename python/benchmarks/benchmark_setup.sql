CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS benchmark_state;
CREATE TABLE benchmark_state (
    key TEXT PRIMARY KEY,
    state TEXT
);

DROP TABLE IF EXISTS benchmark_data;
DROP SEQUENCE IF EXISTS benchmark_data_id_seq;
CREATE SEQUENCE benchmark_data_id_seq;
CREATE TABLE benchmark_data (
    id INT PRIMARY KEY DEFAULT nextval('benchmark_data_id_seq'),
    value BYTEA
);

DROP TABLE IF EXISTS benchmark_data_with_index;
DROP SEQUENCE IF EXISTS benchmark_data_with_index_id_seq;
CREATE SEQUENCE benchmark_data_with_index_id_seq;
CREATE TABLE benchmark_data_with_index (
    id INT PRIMARY KEY DEFAULT nextval('benchmark_data_with_index_id_seq'),
    value INT
);

CREATE INDEX bb_data_idx on benchmark_data_with_index(value);

-- Insert sentinel value
INSERT INTO benchmark_state (key, state)
VALUES ('common_benchmark_setup', 'ready');
