## test
CREATE TABLE IF NOT EXISTS table_with_geom_types_insert (
    id SERIAL PRIMARY KEY,
    point_geom POINT,
    line_geom LINE,
    line_segment_geom LSEG,
    box_geom BOX,
    path_geom PATH,
    polygon_geom POLYGON,
    circle_geom CIRCLE
);

INSERT INTO table_with_geom_types_insert (
    point_geom,
    line_geom,
    line_segment_geom,
    box_geom,
    path_geom,
    polygon_geom,
    circle_geom
) VALUES
(
    '(3.0, 4.0)', '{3,4,5}', '[(2,3),(4,5)]', '(1,1),(2,2)', '((1,1),(2,2),(3,1))',
    '((0,0),(2,0),(2,2),(0,2),(0,0))', '<(2,2),5>'
),
(
    '(5.0, 6.0)', '{5,6,7}', '[(3,4),(5,6)]', '(2,2),(3,3)', '((2,2),(3,3),(4,2))',
    '((1,1),(3,1),(3,3),(1,3),(1,1))', '<(3,3),7>'
);

## verify
SELECT * FROM table_with_geom_types_insert;

## cleanup
DROP TABLE IF EXISTS table_with_geom_types_insert;
