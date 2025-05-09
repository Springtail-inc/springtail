## test
DROP SCHEMA IF EXISTS hollywood CASCADE;

CREATE SCHEMA hollywood
    CREATE TABLE films (title text, release date, awards text[]);

INSERT INTO hollywood.films (title, release, awards)
VALUES
    ('Forrest Gump', '1994-07-06', ARRAY['Best Picture', 'Best Actor', 'Best Director']),
    ('The Dark Knight', '2008-07-18', ARRAY['Best Supporting Actor']),
    ('Inception', '2010-07-16', ARRAY['Best Cinematography', 'Best Sound Editing']),
    ('La La Land', '2016-12-09', ARRAY['Best Director', 'Best Actress']),
    ('The Room', '2003-06-27', NULL),
    ('No Country for Old Men', '2007-11-21', ARRAY['Best Picture', 'Best Director']),
    ('Titanic', '1997-12-19', ARRAY['Best Picture', 'Best Director', 'Best Original Song']),
    ('Interstellar', '2014-11-07', ARRAY['Best Visual Effects']),
    ('The Irishman', '2019-11-01', NULL),
    ('Parasite', '2019-05-30', ARRAY['Best Picture', 'Best Director', 'Best Original Screenplay']);

CREATE SCHEMA world
    CREATE TABLE countries (
        country_id CHARACTER (2) PRIMARY KEY,
        country_name CHARACTER VARYING (40),
        region_id INTEGER NOT NULL
    )
    CREATE INDEX ON countries(country_name ASC);

INSERT INTO world.countries (country_id, country_name, region_id)
VALUES
    ('AL', 'Albania', 1),
    ('AD', 'Andorra', 1),
    ('AM', 'Armenia', 1),
    ('AT', 'Austria', 1),
    ('BY', 'Belarus', 1),
    ('BE', 'Belgium', 1),
    ('BA', 'Bosnia and Herzegovina', 1),
    ('BG', 'Bulgaria', 1),
    ('HR', 'Croatia', 1),
    ('CY', 'Cyprus', 1),
    ('CZ', 'Czech Republic', 1);


## verify
### schema_check hollywood films
### schema_check world countries
SELECT * FROM hollywood.films;
SELECT * FROM world.countries ORDER BY country_id;

## cleanup
DROP SCHEMA hollywood CASCADE;
DROP SCHEMA world CASCADE;
