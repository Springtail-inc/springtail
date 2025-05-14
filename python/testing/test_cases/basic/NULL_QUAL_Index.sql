## test
CREATE TABLE test_data (
    id serial PRIMARY KEY,
    a int2,
    b int4,
    c int8,
    aa int2
);

create index test_data_a_idx on test_data(a);
create index test_data_b_idx on test_data(b);
create index test_data_c_idx on test_data(c);

-- Insert
INSERT INTO test_data (a, b, c)
SELECT
    (g % 32000)::int2,
    g::int4,
    g::int8
FROM generate_series(1, 1000) AS g;

INSERT INTO test_data (a, b, c)
SELECT
    NULL,
    NULL,
    NULL
FROM generate_series(1, 10) AS g;

## verify
SELECT * FROM test_data ORDER BY id;

SELECT * FROM test_data WHERE a IS NULL ORDER BY id;
SELECT * FROM test_data WHERE a IS NOT NULL ORDER BY id;
SELECT * FROM test_data WHERE b IS NULL ORDER BY id;
SELECT * FROM test_data WHERE b IS NOT NULL ORDER BY id;
SELECT * FROM test_data WHERE b IS NULL ORDER BY id;
SELECT * FROM test_data WHERE b IS NOT NULL ORDER BY id;

SELECT * FROM test_data WHERE aa = 1::int2;
SELECT * FROM test_data WHERE a = 1::int2;
SELECT * FROM test_data WHERE a = 1::int4;
SELECT * FROM test_data WHERE a = 1::int8;
SELECT * FROM test_data WHERE a = 4560050::int4;
SELECT * FROM test_data WHERE b = 23::int2;
SELECT * FROM test_data WHERE b = 23::int8;
SELECT * FROM test_data WHERE c = 23::int2;
SELECT * FROM test_data WHERE c = 23::int4;

## cleanup
DROP TABLE IF EXISTS test_data CASCADE;