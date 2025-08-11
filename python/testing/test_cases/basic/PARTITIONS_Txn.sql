## test

CREATE TABLE animals (
    id SERIAL,
    name TEXT,
    type TEXT,
    PRIMARY KEY (id, type)
) PARTITION BY LIST (type);

CREATE TABLE animals_carnivore (
    id SERIAL,
    name TEXT,
    type TEXT,
    PRIMARY KEY (id, type)
);

CREATE TABLE animals_herbivore (
    id SERIAL,
    name TEXT,
    type TEXT,
    PRIMARY KEY (id, type)
);

BEGIN;
ALTER TABLE animals ATTACH PARTITION animals_carnivore FOR VALUES IN ('Carnivore');
INSERT INTO animals_carnivore (name, type) VALUES ('Lion', 'Carnivore');
INSERT INTO animals_carnivore (name, type) VALUES ('Tiger', 'Carnivore');
INSERT INTO animals_carnivore (name, type) VALUES ('Leopard', 'Carnivore');
COMMIT;

BEGIN;
ALTER TABLE animals ATTACH PARTITION animals_herbivore FOR VALUES IN ('Herbivore');
INSERT INTO animals_herbivore (name, type) VALUES ('Cow', 'Herbivore');
INSERT INTO animals_herbivore (name, type) VALUES ('Goat', 'Herbivore');
INSERT INTO animals_herbivore (name, type) VALUES ('Sheep', 'Herbivore');
COMMIT;


## verify
### schema_check public animals
### schema_check public animals_carnivore
### schema_check public animals_herbivore
SELECT * FROM animals ORDER BY id;

## cleanup
SELECT 1;
