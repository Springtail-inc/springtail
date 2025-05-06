## test
BEGIN;

CREATE TABLE IF NOT EXISTS regions (
    region_id SERIAL PRIMARY KEY,
    region_name CHARACTER VARYING (25)
);

CREATE TABLE IF NOT EXISTS countries (
    country_id CHARACTER (2) PRIMARY KEY,
    country_name CHARACTER VARYING (40),
    region_id INTEGER NOT NULL,
    FOREIGN KEY (region_id) REFERENCES regions (region_id) ON UPDATE CASCADE ON DELETE CASCADE
);

COMMIT;

-- INSERTING REGIONS --
BEGIN;
INSERT INTO regions (region_id, region_name) VALUES (1, 'Europe');
INSERT INTO regions (region_id, region_name) VALUES (2, 'North America');
INSERT INTO regions (region_id, region_name) VALUES (3, 'South America');
INSERT INTO regions (region_id, region_name) VALUES (4, 'Central America');
INSERT INTO regions (region_id, region_name) VALUES (5, 'Asia');
INSERT INTO regions (region_id, region_name) VALUES (6, 'Africa');
INSERT INTO regions (region_id, region_name) VALUES (7, 'Oceania');
INSERT INTO regions (region_id, region_name) VALUES (8, 'The Caribbean');
COMMIT;

-- INSERTING SOME COUNTRIES --
BEGIN;

INSERT INTO countries (country_id, country_name, region_id) VALUES ('DK', 'Denmark', 1);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('EE', 'Estonia', 1);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('NL', 'Netherlands', 1);

INSERT INTO countries (country_id, country_name, region_id) VALUES ('CA', 'Canada', 2);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('US', 'United States', 2);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('MX', 'Mexico', 2);

INSERT INTO countries (country_id, country_name, region_id) VALUES ('AR', 'Argentina', 3);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('BR', 'Brazil', 3);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('CL', 'Chile', 3);

INSERT INTO countries (country_id, country_name, region_id) VALUES ('SV', 'El Salvador', 4);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('GT', 'Guatemala', 4);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('HN', 'Honduras', 4);

INSERT INTO countries (country_id, country_name, region_id) VALUES ('KZ', 'Kazakhstan', 5);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('KP', 'North Korea', 5);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('AE', 'United Arab Emirates', 5);

INSERT INTO countries (country_id, country_name, region_id) VALUES ('CF', 'Central African Republic', 6);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('MA', 'Morocco', 6);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('ZA', 'South Africa', 6);

INSERT INTO countries (country_id, country_name, region_id) VALUES ('AU', 'Australia', 7);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('NZ', 'New Zealand', 7);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('PG', 'Papua New Guinea', 7);

INSERT INTO countries (country_id, country_name, region_id) VALUES ('CU', 'Cuba', 8);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('PR', 'Puerto Rico', 8);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('SX', 'Sint Maarten', 8);

COMMIT;

-- DDLs without DMLs without resync--
BEGIN;

DROP TABLE countries;
CREATE TABLE new_countries (
    country_id CHARACTER (2) PRIMARY KEY,
    new_country_name CHARACTER VARYING (40),
    region_id INTEGER NOT NULL,
    FOREIGN KEY (region_id) REFERENCES regions (region_id) ON UPDATE CASCADE ON DELETE CASCADE
);

ALTER TABLE new_countries RENAME COLUMN new_country_name TO country_name;

CREATE INDEX country_name_index ON new_countries (country_name);

SELECT pg_sleep(3);

DROP INDEX country_name_index;

COMMIT;

-- DDLs with DMLs without resync --
BEGIN;

DROP TABLE new_countries;
CREATE TABLE countries (
    country_id CHARACTER (2) PRIMARY KEY,
    new_country_name CHARACTER VARYING (40),
    region_id INTEGER NOT NULL,
    FOREIGN KEY (region_id) REFERENCES regions (region_id) ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('DK', 'Denmark', 1);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('EE', 'Estonia', 1);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('NL', 'Netherlands', 1);

INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('CA', 'Canada', 2);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('US', 'United States', 2);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('MX', 'Mexico', 2);

INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('AR', 'Argentina', 3);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('BR', 'Brazil', 3);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('CL', 'Chile', 3);

INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('SV', 'El Salvador', 4);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('GT', 'Guatemala', 4);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('HN', 'Honduras', 4);

INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('KZ', 'Kazakhstan', 5);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('KP', 'North Korea', 5);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('AE', 'United Arab Emirates', 5);

INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('CF', 'Central African Republic', 6);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('MA', 'Morocco', 6);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('ZA', 'South Africa', 6);

INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('AU', 'Australia', 7);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('NZ', 'New Zealand', 7);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('PG', 'Papua New Guinea', 7);

INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('CU', 'Cuba', 8);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('PR', 'Puerto Rico', 8);
INSERT INTO countries (country_id, new_country_name, region_id) VALUES ('SX', 'Sint Maarten', 8);

ALTER TABLE countries RENAME COLUMN new_country_name TO country_name;

INSERT INTO countries (country_id, country_name, region_id) VALUES ('AM', 'Armenia', 1);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('CO', 'Colombia', 3);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('PA', 'Panama', 4);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('AZ', 'Azerbaijan', 5);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('CI', 'Côte d''Ivoire', 6);

CREATE INDEX country_name_index ON countries (country_name);

SELECT pg_sleep(3);

DELETE FROM countries WHERE country_name = 'Azerbaijan';
UPDATE countries SET country_name = 'Free Republic of North Korea' WHERE country_id = 'KP';

DROP INDEX country_name_index;

COMMIT;

-- DDLs with DMLs with resync --
BEGIN;

-- add columns
ALTER TABLE countries
    ADD COLUMN area INTEGER CHECK (area >= 0) DEFAULT 0 NOT NULL,
    ADD COLUMN population BIGINT CHECK (population >= 0) DEFAULT 0 NOT NULL;

UPDATE countries SET area = 429, population = 5831404 WHERE country_id = 'DK';
UPDATE countries SET area = 45227, population = 1326535 WHERE country_id = 'EE';
UPDATE countries SET area = 41833, population = 17019800 WHERE country_id = 'NL';

-- remove columns
ALTER TABLE countries ALTER COLUMN area SET DATA TYPE BIGINT;

UPDATE countries SET area = 9833517, population = 331002651 WHERE country_id = 'US';
UPDATE countries SET area = 9984670, population = 37742154 WHERE country_id = 'CA';
UPDATE countries SET area = 1964375, population = 128933195 WHERE country_id = 'MX';

-- alter column type
ALTER TABLE countries DROP COLUMN population;
ALTER TABLE countries DROP COLUMN area;

COMMIT;

-- add another table
BEGIN;

CREATE TABLE IF NOT EXISTS locations (
    location_id SERIAL PRIMARY KEY,
    street_address CHARACTER VARYING (40),
    postal_code CHARACTER VARYING (12),
    city CHARACTER VARYING (30) NOT NULL,
    state_province CHARACTER VARYING (35),
    country_id CHARACTER (2) NOT NULL,
    FOREIGN KEY (country_id) REFERENCES countries (country_id) ON UPDATE CASCADE ON DELETE CASCADE
);

COMMIT;

-- DDLs with DMLs with resync and other table DMLs --
BEGIN;

-- INSERTING LOCATION for Denmark --
INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (12, 'Østerbrogade 45', '2100', 'Copenhagen', 'Capital Region', 'DK');

-- add columns
ALTER TABLE countries
    ADD COLUMN area INTEGER CHECK (area >= 0) DEFAULT 0 NOT NULL,
    ADD COLUMN population BIGINT CHECK (population >= 0) DEFAULT 0 NOT NULL;

-- INSERTING LOCATION for Estonia --
INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (13, 'Pärnu mnt 18', '10141', 'Tallinn', 'Harju County', 'EE');

UPDATE countries SET area = 429, population = 5831404 WHERE country_id = 'DK';
UPDATE countries SET area = 45227, population = 1326535 WHERE country_id = 'EE';
UPDATE countries SET area = 41833, population = 17019800 WHERE country_id = 'NL';

-- INSERTING LOCATION for Netherlands --
INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (32, 'Damrak 70', '1012 LM', 'Amsterdam', 'North Holland', 'NL');

-- remove columns
ALTER TABLE countries ALTER COLUMN area SET DATA TYPE BIGINT;

 -- UPDATING LOCATION for Denmark --
UPDATE locations SET street_address = 'Østerbrogade 50' WHERE country_id = 'DK';

UPDATE countries SET area = 9833517, population = 331002651 WHERE country_id = 'US';
UPDATE countries SET area = 9984670, population = 37742154 WHERE country_id = 'CA';
UPDATE countries SET area = 1964375, population = 128933195 WHERE country_id = 'MX';

 -- UPDATING LOCATION for Estonia --
UPDATE locations SET street_address = 'Pärnu mnt 18' WHERE country_id = 'EE';

-- alter column type
ALTER TABLE countries DROP COLUMN population;
ALTER TABLE countries DROP COLUMN area;

 -- UPDATING LOCATION for Netherlands --
UPDATE locations SET street_address = 'Damrak 70' WHERE country_id = 'NL';

COMMIT;

## verify
SELECT * FROM regions ORDER BY region_id;
SELECT * FROM countries ORDER BY country_id;

## cleanup
BEGIN; 
DROP TABLE IF EXISTS countries CASCADE;
DROP TABLE IF EXISTS regions CASCADE;
COMMIT;

