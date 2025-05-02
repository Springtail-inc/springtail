## test
CREATE TYPE colors AS ENUM ('red', 'green', 'blue');
CREATE TABLE balloons (
    id SERIAL PRIMARY KEY,
    color colors
);
INSERT INTO balloons (color) VALUES ('blue'), ('green'), ('red');

## verify
SELECT * FROM balloons ORDER BY id;

SELECT id FROM balloons WHERE color = 'blue';
SELECT id FROM balloons WHERE color <> 'green' ORDER BY ID;
SELECT id FROM balloons WHERE color > 'green' ORDER BY ID;
SELECT color FROM balloons order by color;

## cleanup
DROP TABLE IF EXISTS balloons;
DROP TYPE IF EXISTS colors;
