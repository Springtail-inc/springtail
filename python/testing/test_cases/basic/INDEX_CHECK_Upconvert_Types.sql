## test
CREATE TABLE index_check(
    id SERIAL8 PRIMARY KEY,
    id_int INT4 NOT NULL,
    id_float FLOAT8 NOT NULL
);

CREATE INDEX index_check_id_int ON index_check(id_int);
CREATE INDEX index_check_id_float ON index_check(id_float);

INSERT INTO index_check (id_int, id_float) VALUES (1, 1.1), (2, 2.2), (3, 3.3);
INSERT INTO index_check (id_int, id_float) VALUES (4, 4.4), (5, 5.5), (6, 6.6);
INSERT INTO index_check (id_int, id_float) VALUES (7, 7.7), (8, 8.8), (9, 9.9);

## verify

SELECT * FROM index_check ORDER BY id;

SELECT * FROM index_check WHERE id = 5::int4;
SELECT * FROM index_check WHERE id = 5::int2;
SELECT * FROM index_check WHERE id_int = 5::int2;
SELECT * FROM index_check WHERE id_float = 5.5::float4;

SELECT * FROM index_check WHERE id > 5::int4 ORDER BY id;
SELECT * FROM index_check WHERE id > 5::int2 ORDER BY id;
SELECT * FROM index_check WHERE id_int > 5::int2 ORDER BY id;
SELECT * FROM index_check WHERE id_float > 5.5::float4 ORDER BY id;

## cleanup
DROP TABLE IF EXISTS index_check CASCADE;