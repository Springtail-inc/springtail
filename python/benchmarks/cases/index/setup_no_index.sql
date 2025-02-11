DROP TABLE IF EXISTS index_test_data;
CREATE TABLE index_test_data(
    col1 INT PRIMARY KEY,
    col2 INT NOT NULL
);

DO $FN$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..10000 LOOP
        INSERT INTO index_test_data (col1, col2) SELECT  i, rnd FROM (SELECT floor(random() * 10000) + 1 AS rnd);
    END LOOP;
END;
$FN$;

