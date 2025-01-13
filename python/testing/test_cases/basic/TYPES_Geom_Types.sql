## test
CREATE TABLE IF NOT EXISTS table_with_geom_types (
    point_geom POINT,
    line_geom LINE,
    line_segment_geom LSEG,
    box_geom BOX,
    path_geom PATH,
    polygon_geom POLYGON,
    circle_geom CIRCLE
);

## verify
### schema_check public table_with_geom_types

## cleanup
DROP TABLE IF EXISTS table_with_geom_types;
