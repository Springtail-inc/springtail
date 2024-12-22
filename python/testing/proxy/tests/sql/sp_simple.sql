DROP TABLE IF EXISTS regress_1;
CREATE table regress_1 (a int primary key, b text);
INSERT INTO regress_1 VALUES
    (1, 'one'),
    (2, 'two'),
    (3, 'three'),
    (4, 'four'),
    (5, 'five'),
    (6, 'six'),
    (7, 'seven'),
    (8, 'eight'),
    (9, 'nine'),
    (10, 'ten');

SELECT * FROM regress_1 ORDER BY a DESC;

BEGIN;
INSERT INTO regress_1 VALUES (11, 'eleven');
SELECT * FROM regress_1 where a > 5 ORDER BY a DESC;
COMMIT;

BEGIN;
INSERT INTO regress_1 VALUES (12, 'twelve');
SELECT * FROM regress_1 where a > 5 ORDER BY a DESC;
ROLLBACK;

SELECT * FROM regress_1 ORDER BY a DESC;