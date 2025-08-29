## test

-- Existing tables created as part of __config.sql
ALTER TABLE movies_2000s DETACH PARTITION genre_2000s_action;

## verify
### schema_check public movies_2000s
### schema_check public genre_2000s_action
SELECT * FROM movies ORDER BY id;

## cleanup
SELECT 1;
