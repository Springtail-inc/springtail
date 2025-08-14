-- copy table test for text index and large text columns
## setup

CREATE TABLE test_it (hash TEXT PRIMARY KEY, a TEXT, b TEXT, c INTEGER);

INSERT INTO test_it (hash, a, b, c)
SELECT
    gen_random_uuid()::text AS hash,
    rpad(generate_series::varchar, 1000, 'hi') as a,
    rpad(generate_series::varchar, 1000, 'by') AS b,
    (random() * 1000)::int AS c
FROM generate_series(1, 1000000);

CREATE TABLE test_it2 (hash TEXT PRIMARY KEY, a TEXT, b TEXT, c INTEGER);

INSERT INTO test_it2 (hash, a, b, c)
SELECT
    gen_random_uuid()::text AS hash,
    md5(random()::text) AS a,
    md5(random()::text) AS b,
    (random() * 1000)::int AS c
FROM generate_series(1, 1000000);

## cleanup
DROP TABLE test_it;
DROP TABLE test_it2;
