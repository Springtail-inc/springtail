## test
CREATE TYPE colors AS ENUM ('red', 'green', 'blue', 'yellow', 'purple', 'orange', 'violet');
CREATE TABLE balloons (
    id SERIAL PRIMARY KEY,
    color colors
);
CREATE INDEX idx_balloons_color ON balloons (color);

INSERT INTO balloons (color) VALUES ('blue'), ('green'), ('red'), ('yellow'), ('purple');
INSERT INTO balloons (color) VALUES ('orange'), ('violet');
INSERT INTO balloons (color) VALUES ('red'), ('green'), ('blue'), ('yellow'), ('purple');
INSERT INTO balloons (color) VALUES ('orange'), ('violet');
INSERT INTO balloons (color) VALUES ('red'), ('green'), ('blue'), ('yellow'), ('purple');
INSERT INTO balloons (color) VALUES ('orange'), ('violet');

## verify
SELECT * FROM balloons ORDER BY id;

SELECT id FROM balloons WHERE color = 'blue';
SELECT id FROM balloons WHERE color <> 'green' ORDER BY ID;
SELECT id FROM balloons WHERE color > 'green' ORDER BY ID;
SELECT color FROM balloons order by color;

## cleanup
DROP TABLE IF EXISTS balloons;
DROP TYPE IF EXISTS colors;