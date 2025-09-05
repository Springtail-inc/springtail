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

CREATE TABLE sample_data (
    id SERIAL PRIMARY KEY,
    col1 VARCHAR(100),
    col2 INTEGER,
    col3 DATE,
    col4 BOOLEAN,
    col5 TEXT,
    col6 NUMERIC(10,2),
    col7 TIMESTAMP DEFAULT NOW(),
    col8 VARCHAR(255),
    col9 BIGINT,
    col10 SMALLINT,
    col11 REAL,
    col12 DOUBLE PRECISION,
    col13 JSONB,
    col14 UUID DEFAULT gen_random_uuid(),
    col15 CHAR(10),
    col16 BYTEA,
    col17 TIME,
    col18 INTERVAL,
    col19 VARCHAR(50),
    col20 TEXT
);

INSERT INTO sample_data (
    col1, col2, col3, col4, col5,
    col6, col7, col8, col9, col10,
    col11, col12, col13, col14, col15,
    col16, col17, col18, col19, col20
)
SELECT
    'text_' || g::text AS col1,
    (random() * 1000)::int AS col2,
    (DATE '2020-01-01' + (g % 365))::date AS col3,
    (g % 2 = 0) AS col4,
    'longer text ' || g::text AS col5,
    random() * 10000 AS col6,
    NOW() - (g || ' minutes')::interval AS col7,
    md5(g::text) AS col8,
    g * 1000 AS col9,
    (g % 32767)::smallint AS col10,
    random()::real AS col11,
    random()::double precision AS col12,
    jsonb_build_object('key', g, 'val', md5(g::text)) AS col13,
    gen_random_uuid() AS col14,
    lpad(g::text, 10, '0') AS col15,
    decode(md5(g::text), 'hex') AS col16,
    (NOW() + (g || ' seconds')::interval)::time AS col17,
    (g || ' days')::interval AS col18,
    'short_' || g::text AS col19,
     (select string_agg(md5(random()::text), '') from generate_series(1,257)) AS col20
FROM generate_series(1, 1000) g;

## cleanup
DROP TABLE movies CASCADE;
DROP TABLE sample_data;
