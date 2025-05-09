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

## verify
### schema_check hollywood films
SELECT * FROM hollywood.films;

## cleanup
DROP SCHEMA hollywood CASCADE;
