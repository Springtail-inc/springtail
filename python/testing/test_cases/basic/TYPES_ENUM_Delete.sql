## test
CREATE TYPE colors AS ENUM ('red', 'green', 'blue');
CREATE TABLE balloons (
    id SERIAL PRIMARY KEY,
    color colors
);
INSERT INTO balloons (color) VALUES ('red'), ('green'), ('blue');
UPDATE balloons SET color = 'blue' WHERE color = 'red';

DELETE FROM balloons WHERE color = 'green';

## verify
SELECT * FROM balloons ORDER BY id;

## cleanup
DROP TABLE IF EXISTS balloons;
DROP TYPE IF EXISTS colors;