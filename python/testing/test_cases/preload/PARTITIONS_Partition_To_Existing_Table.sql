## test
### switch_db partitions
-- Existing tables created in __config.sql
CREATE TABLE genre_2020s_animation PARTITION OF movies_2020s FOR VALUES IN ('Animation');

## verify
### switch_db partitions
### schema_check public movies_2000s
### schema_check public movies_2000s_animation
SELECT * FROM movies ORDER BY id;

## cleanup
SELECT 1;
