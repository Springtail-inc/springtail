## test
CREATE TABLE family (
    id SERIAL,
    name TEXT NOT NULL,
    age INT NOT NULL,
    relationship TEXT NOT NULL CHECK (relationship IN ('Cousin', 'Grandparent', 'Sibling')),
    gender TEXT NOT NULL,
    PRIMARY KEY (relationship, id)
) PARTITION BY LIST (relationship);

CREATE TABLE family_cousins PARTITION OF family FOR VALUES IN ('Cousin');
CREATE TABLE family_grandparents PARTITION OF family FOR VALUES IN ('Grandparent');
CREATE TABLE family_siblings PARTITION OF family FOR VALUES IN ('Sibling');

INSERT INTO family (name, age, relationship, gender) VALUES
    ('John Doe', 30, 'Cousin', 'Male'),
    ('Jane Doe', 25, 'Cousin', 'Female'),
    ('Bob Smith', 50, 'Grandparent', 'Male'),
    ('Alice Smith', 45, 'Grandparent', 'Female'),
    ('Tom Johnson', 28, 'Sibling', 'Male'),
    ('Sara Johnson', 26, 'Sibling', 'Female');

ALTER TABLE family DETACH PARTITION family_cousins;

## verify
SELECT * FROM family;

## cleanup
DROP TABLE family CASCADE;
