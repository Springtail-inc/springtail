## test
CREATE TYPE colors AS ENUM ('red', 'green', 'blue');
CREATE TABLE balloons (
    id SERIAL PRIMARY KEY,
    color colors
);
INSERT INTO balloons (color) VALUES ('red'), ('green'), ('blue');

## verify
SELECT * FROM balloons ORDER BY id;

SELECT id FROM balloons WHERE color = 'blue';
SELECT id FROM balloons WHERE color <> 'green' ORDER BY ID;

## cleanup
DROP TABLE IF EXISTS balloons;
DROP TYPE IF EXISTS colors;


create type mood as enum('good','bad', 'ok');

create table person(name text primary key, mood mood);

insert into person values('John','good'),('Joe','ok');

alter type mood rename value 'ok' to 'sad';

alter type mood add value 'great' before 'sad';