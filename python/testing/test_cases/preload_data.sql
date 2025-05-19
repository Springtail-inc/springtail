-- create table with toast data
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS preload_random_binary_test (
    id SERIAL PRIMARY KEY,
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS preload_random_text_test (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL
);

-- insert into preload_random_binary_test
WITH chunk AS (SELECT gen_random_bytes(1024) AS b)
INSERT INTO preload_random_binary_test (data)
SELECT b || b || b || b || b || b || b || b || b || b FROM chunk
CROSS JOIN generate_series(1, 10);

-- insert into preload_random_text_test
INSERT INTO preload_random_text_test (data)
SELECT string_agg(chr(65 + trunc(random() * 94)::int), '')
FROM generate_series(1, 10) AS gs1(id)
JOIN generate_series(1, 100000) AS gs2(id2) ON TRUE
GROUP BY gs1.id;

