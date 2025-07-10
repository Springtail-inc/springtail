## test
CREATE TABLE grandparent (
    id INT,
    name TEXT,
    type TEXT CHECK (type IN ('TYPE_1', 'TYPE_2')),
    category TEXT CHECK (category IN ('ABC', 'DEF')),
    count INT CHECK (count IN (1, 2))
) PARTITION BY LIST (type);

CREATE TABLE parent1 PARTITION OF grandparent FOR VALUES IN ('TYPE_1')
PARTITION BY LIST (category);
CREATE TABLE parent2 PARTITION OF grandparent FOR VALUES IN ('TYPE_2')
PARTITION BY LIST (category);

CREATE TABLE child1 PARTITION OF parent1 FOR VALUES IN ('ABC')
PARTITION BY LIST (count);
CREATE TABLE child2 PARTITION OF parent1 FOR VALUES IN ('DEF')
PARTITION BY LIST (count);

CREATE TABLE child4 PARTITION OF parent2 FOR VALUES IN ('ABC')
PARTITION BY LIST (count);
CREATE TABLE child5 PARTITION OF parent2 FOR VALUES IN ('DEF')
PARTITION BY LIST (count);

CREATE TABLE grandchild1 PARTITION OF child1 FOR VALUES IN (1);
CREATE TABLE grandchild2 PARTITION OF child1 FOR VALUES IN (2);

CREATE TABLE grandchild3 PARTITION OF child2 FOR VALUES IN (1);
CREATE TABLE grandchild4 PARTITION OF child2 FOR VALUES IN (2);

CREATE TABLE grandchild5 PARTITION OF child4 FOR VALUES IN (1);
CREATE TABLE grandchild6 PARTITION OF child4 FOR VALUES IN (2);

CREATE TABLE grandchild7 PARTITION OF child5 FOR VALUES IN (1);
CREATE TABLE grandchild8 PARTITION OF child5 FOR VALUES IN (2);

INSERT INTO grandparent (id, name, type, category, count)
VALUES
    (1, 'Name1', 'TYPE_1', 'ABC', 1),
    (2, 'Name2', 'TYPE_1', 'DEF', 2),
    (3, 'Name3', 'TYPE_1', 'ABC', 1),
    (4, 'Name4', 'TYPE_2', 'ABC', 2),
    (5, 'Name5', 'TYPE_2', 'DEF', 1),
    (6, 'Name6', 'TYPE_2', 'DEF', 2);

ALTER TABLE child1 DETACH PARTITION grandchild1;
ALTER TABLE grandparent DETACH PARTITION parent2;

## verify
### schema_check public grandparent
### schema_check public parent1
### schema_check public parent2
### schema_check public child1
### schema_check public child2
### schema_check public grandchild1
### schema_check public grandchild2
### schema_check public grandchild3
### schema_check public grandchild4
SELECT * FROM grandparent ORDER BY id;

## cleanup
DROP TABLE grandparent CASCADE;
