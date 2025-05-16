## test
CREATE TABLE test_data2 (
    id serial PRIMARY KEY,
    d float4,
    e float8,
    f numeric
);

create index test_data2_d_idx on test_data2(d);
create index test_data2_e_idx on test_data2(e);
create index test_data2_f_idx on test_data2(f);

-- Insert
INSERT INTO test_data2 (d, e, f)
SELECT
    g::float4,
    g::float8,
    g::numeric
FROM generate_series(1, 1000) AS g;

INSERT INTO test_data2 (d, e, f)
SELECT
    NULL,
    NULL,
    NULL
FROM generate_series(1, 10) AS g;

## verify
SELECT * FROM test_data2 ORDER BY id;

SELECT * FROM test_data2 WHERE d IS NULL ORDER BY id;
SELECT * FROM test_data2 WHERE d IS NOT NULL ORDER BY id;
SELECT * FROM test_data2 WHERE e IS NULL ORDER BY id;
SELECT * FROM test_data2 WHERE e IS NOT NULL ORDER BY id;
SELECT * FROM test_data2 WHERE f IS NULL ORDER BY id;
SELECT * FROM test_data2 WHERE f IS NOT NULL ORDER BY id;

SELECT * FROM test_data2 WHERE d = 23::int2;
SELECT * FROM test_data2 WHERE d = 23::int4;
SELECT * FROM test_data2 WHERE d = 23::float8;
SELECT * FROM test_data2 WHERE e = 23.0::float4;
SELECT * FROM test_data2 WHERE f = 23::numeric;
SELECT * FROM test_data2 WHERE f = 23::int4;

## cleanup
DROP TABLE IF EXISTS test_data2 CASCADE;
