## test
CREATE TABLE test_data (
    id serial PRIMARY KEY,
    d float4,
    e float8,
    f numeric
);

create index test_data_d_idx on test_data(d);
create index test_data_e_idx on test_data(e);
create index test_data_f_idx on test_data(f);

-- Insert
INSERT INTO test_data (d, e, f)
SELECT
    g::float4,
    g::float8,
    g::numeric
FROM generate_series(1, 1000) AS g;

INSERT INTO test_data (d, e, f)
SELECT
    NULL,
    NULL,
    NULL
FROM generate_series(1, 10) AS g;

## verify
SELECT * FROM test_data ORDER BY id;

SELECT * FROM test_data WHERE d IS NULL ORDER BY id;
SELECT * FROM test_data WHERE d IS NOT NULL ORDER BY id;
SELECT * FROM test_data WHERE e IS NULL ORDER BY id;
SELECT * FROM test_data WHERE e IS NOT NULL ORDER BY id;
SELECT * FROM test_data WHERE f IS NULL ORDER BY id;
SELECT * FROM test_data WHERE f IS NOT NULL ORDER BY id;

SELECT * FROM test_data WHERE d = 23::int2;
SELECT * FROM test_data WHERE d = 23::int4;
SELECT * FROM test_data WHERE d = 23::float8;
SELECT * FROM test_data WHERE e = 23.0::float4;
SELECT * FROM test_data WHERE f = 23::numeric;
SELECT * FROM test_data WHERE f = 23::int4;

## cleanup
DROP TABLE IF EXISTS test_data CASCADE;
