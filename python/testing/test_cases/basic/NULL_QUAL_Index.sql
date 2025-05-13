## test
CREATE TABLE test_data (
    id serial PRIMARY KEY,
    a int2,
    b int4,
    c int8,
    d float4,
    e float8,
    f numeric,
    aa int2,
    bb int4,
    cc int8,
    dd float4,
    ee float8,
    ff numeric
);

create index test_data_a_idx on test_data(a);
create index test_data_b_idx on test_data(b);
create index test_data_c_idx on test_data(c);
create index test_data_d_idx on test_data(d);
create index test_data_e_idx on test_data(e);
create index test_data_f_idx on test_data(f);

-- Insert
INSERT INTO test_data (a, b, c, d, e, f, aa, bb, cc, dd, ee, ff)
SELECT
    (g % 32000)::int2,
    g::int4,
    g::int8,
    g::float4,
    g::float8,
    g::numeric,
    (g % 32000)::int2,
    g::int4,
    g::int8,
    g::float4,
    g::float8,
    g::numeric
FROM generate_series(1, 10000) AS g;

INSERT INTO test_data (a, b, c, d, e, f, aa, bb, cc, dd, ee, ff)
SELECT
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
FROM generate_series(1, 100) AS g;

## verify
SELECT * FROM test_data ORDER BY id;
SELECT id FROM test_data WHERE a = 1;
SELECT id FROM test_data WHERE a IS NULL;
SELECT id FROM test_data WHERE a IS NOT NULL;
SELECT id FROM test_data WHERE b IS NULL;
SELECT id FROM test_data WHERE b IS NOT NULL;

SELECT * FROM test_data WHERE b = 23::int2;
SELECT * FROM test_data WHERE c = 23::int4;
SELECT * FROM test_data WHERE d = 23::int2;
SELECT * FROM test_data WHERE e = 23.0::float4;
SELECT * FROM test_data WHERE f = 23::numeric;

SELECT * FROM test_data WHERE f IS NULL;
SELECT * FROM test_data WHERE f IS NOT NULL;

## cleanup
DROP TABLE IF EXISTS test_null_quals CASCADE;