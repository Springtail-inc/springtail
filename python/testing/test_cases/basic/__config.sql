## setup
SELECT 1;

CREATE TABLE movies (
    id SERIAL,
    name TEXT,
    genre TEXT CHECK (genre IN ('Action', 'Comedy', 'Drama', 'Horror', 'Sci-Fi', 'Animation')),
    release_date DATE,
    PRIMARY KEY (id, release_date, genre)
) PARTITION BY RANGE (release_date);

CREATE TABLE movies_90s PARTITION OF movies FOR VALUES FROM ('1990-01-01') TO ('2000-01-01')
    PARTITION BY LIST (genre);
CREATE TABLE movies_2000s PARTITION OF movies FOR VALUES FROM ('2000-01-01') TO ('2010-01-01')
    PARTITION BY LIST (genre);
CREATE TABLE movies_2010s PARTITION OF movies FOR VALUES FROM ('2010-01-01') TO ('2020-01-01')
    PARTITION BY LIST (genre);
CREATE TABLE movies_2020s PARTITION OF movies FOR VALUES FROM ('2020-01-01') TO ('2030-01-01')
    PARTITION BY LIST (genre);

CREATE TABLE genre_90s_action PARTITION OF movies_90s FOR VALUES IN ('Action');
CREATE TABLE genre_90s_comedy PARTITION OF movies_90s FOR VALUES IN ('Comedy');
CREATE TABLE genre_90s_drama PARTITION OF movies_90s FOR VALUES IN ('Drama');
CREATE TABLE genre_90s_horror PARTITION OF movies_90s FOR VALUES IN ('Horror');
CREATE TABLE genre_90s_scifi PARTITION OF movies_90s FOR VALUES IN ('Sci-Fi');

CREATE TABLE genre_2000s_action PARTITION OF movies_2000s FOR VALUES IN ('Action');
CREATE TABLE genre_2000s_comedy PARTITION OF movies_2000s FOR VALUES IN ('Comedy');
CREATE TABLE genre_2000s_drama PARTITION OF movies_2000s FOR VALUES IN ('Drama');
CREATE TABLE genre_2000s_horror PARTITION OF movies_2000s FOR VALUES IN ('Horror');
CREATE TABLE genre_2000s_scifi PARTITION OF movies_2000s FOR VALUES IN ('Sci-Fi');

CREATE TABLE genre_2010s_action PARTITION OF movies_2010s FOR VALUES IN ('Action');
CREATE TABLE genre_2010s_comedy PARTITION OF movies_2010s FOR VALUES IN ('Comedy');
CREATE TABLE genre_2010s_drama PARTITION OF movies_2010s FOR VALUES IN ('Drama');
CREATE TABLE genre_2010s_horror PARTITION OF movies_2010s FOR VALUES IN ('Horror');
CREATE TABLE genre_2010s_scifi PARTITION OF movies_2010s FOR VALUES IN ('Sci-Fi');

CREATE TABLE genre_2020s_action PARTITION OF movies_2020s FOR VALUES IN ('Action');
CREATE TABLE genre_2020s_comedy PARTITION OF movies_2020s FOR VALUES IN ('Comedy');
CREATE TABLE genre_2020s_drama PARTITION OF movies_2020s FOR VALUES IN ('Drama');
CREATE TABLE genre_2020s_horror PARTITION OF movies_2020s FOR VALUES IN ('Horror');
CREATE TABLE genre_2020s_scifi PARTITION OF movies_2020s FOR VALUES IN ('Sci-Fi');

INSERT INTO movies (name, genre, release_date) VALUES
    ('The Dark Knight', 'Action', '2008-07-18'),
    ('The Shawshank Redemption', 'Drama', '1994-09-10'),
    ('The Dark Knight Rises', 'Action', '2012-07-20'),
    ('The Godfather Part III', 'Drama', '2006-06-15'),
    ('Lock, Stock and Two Smoking Barrels', 'Comedy', '1998-01-01'),
    ('The Matrix', 'Sci-Fi', '1999-01-01'),
    ('The Lord of the Rings: The Return of the King', 'Drama', '2003-01-01'),
    ('Hot Tub Time Machine', 'Comedy', '2010-01-01'),
    ('Guardians of the Galaxy', 'Sci-Fi', '2014-08-01'),
    ('Iron Man', 'Action', '2008-05-01'),
    ('Avengers: Endgame', 'Action', '2019-04-26'),
    ('Hobbit: The Battle of the Five Armies', 'Drama', '2014-12-17'),
    ('The Hobbit: The Desolation of Smaug', 'Drama', '2012-12-11'),
    ('The Hobbit: An Unexpected Journey', 'Drama', '2012-11-21');

## cleanup
DROP TABLE movies CASCADE;
