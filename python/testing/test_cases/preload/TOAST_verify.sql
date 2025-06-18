## test

### switch_db toast
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS random_binary_test (
    id SERIAL PRIMARY KEY,
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS random_text_test (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL
);

-- insert into random_binary_test
WITH chunk AS (SELECT gen_random_bytes(1024) AS b)
INSERT INTO random_binary_test (data)
SELECT b || b || b || b || b || b || b || b || b || b FROM chunk
CROSS JOIN generate_series(1, 5);

-- insert into random_text_test
INSERT INTO random_text_test (data)
SELECT string_agg(chr(65 + trunc(random() * 94)::int), '')
FROM generate_series(1, 5) AS gs1(id)
JOIN generate_series(1, 100000) AS gs2(id2) ON TRUE
GROUP BY gs1.id;

-- update table random_binary_test
WITH chunk AS (SELECT gen_random_bytes(1024) AS b)
UPDATE random_binary_test
    SET data = (
      SELECT b || b || b || b || b || b || b || b || b || b FROM chunk
    ) WHERE id = 1;

-- update table random_text_test
WITH chunk AS (
  SELECT string_agg(chr(33 + trunc(random() * 94)::int), '') AS t
  FROM generate_series(1, 10000)
)
UPDATE random_text_test
SET data = (SELECT t FROM chunk)
WHERE id = 1;

-- delete a row from random_binary_test
DELETE FROM random_binary_test WHERE id = 2;

-- delete a row from random_text_test
DELETE FROM random_text_test WHERE id = 2;

## verify
### switch_db toast
SELECT * FROM random_binary_test ORDER BY id;
SELECT data FROM random_binary_test ORDER BY id;
SELECT * FROM random_text_test ORDER BY id;
SELECT data FROM random_text_test ORDER BY id;

SELECT * FROM preload_random_binary_test ORDER BY id;
SELECT * FROM preload_random_text_test ORDER BY id;

## cleanup
### switch_db toast
DROP TABLE IF EXISTS random_binary_test;
DROP TABLE IF EXISTS random_text_test;

