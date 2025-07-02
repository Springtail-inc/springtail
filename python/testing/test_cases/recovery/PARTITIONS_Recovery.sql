## test

CREATE TABLE film_cast (
    id SERIAL,
    name TEXT,
    role TEXT,
    PRIMARY KEY (id, role)
) PARTITION BY LIST (role);

CREATE TABLE film_cast_director (
    id SERIAL,
    name TEXT,
    role TEXT,
    PRIMARY KEY (id, role)
);

CREATE TABLE film_cast_actor (
    id SERIAL,
    name TEXT,
    role TEXT,
    PRIMARY KEY (id, role)
);

CREATE TABLE film_cast_actress (
    id SERIAL,
    name TEXT,
    role TEXT,
    PRIMARY KEY (id, role)
);


ALTER TABLE film_cast ATTACH PARTITION film_cast_director FOR VALUES IN ('Director');

### recovery_point

ALTER TABLE film_cast ATTACH PARTITION film_cast_actor FOR VALUES IN ('Actor');
ALTER TABLE film_cast ATTACH PARTITION film_cast_actress FOR VALUES IN ('Actress');

INSERT INTO film_cast_director (name, role) VALUES ('Chris Columbus', 'Director');
INSERT INTO film_cast_director (name, role) VALUES ('Quentin Tarantino', 'Director');
INSERT INTO film_cast_director (name, role) VALUES ('Christopher Nolan', 'Director');

INSERT INTO film_cast_actor (name, role) VALUES ('Tom Hanks', 'Actor');
INSERT INTO film_cast_actor (name, role) VALUES ('Morgan Freeman', 'Actor');
INSERT INTO film_cast_actor (name, role) VALUES ('John Travolta', 'Actor');
INSERT INTO film_cast_actor (name, role) VALUES ('Johnny Depp', 'Actor');

INSERT INTO film_cast_actress (name, role) VALUES ('Meryl Streep', 'Actress');
INSERT INTO film_cast_actress (name, role) VALUES ('Scarlett Johansson', 'Actress');
INSERT INTO film_cast_actress (name, role) VALUES ('Anne Hathaway', 'Actress');

### force_recovery

ALTER TABLE film_cast DETACH PARTITION film_cast_director;

## verify
### schema_check public film_cast
### schema_check public film_cast_director
### schema_check public film_cast_actor
### schema_check public film_cast_actress
SELECT * FROM film_cast ORDER BY id;

## cleanup
SELECT 1;
