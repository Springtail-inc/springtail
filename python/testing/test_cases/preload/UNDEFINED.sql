## test
### switch_db sample_db
UPDATE sample_data SET col9 = col9 + 2 WHERE id < 100;

## verify
### switch_db sample_db
SELECT * from sample_data where id < 100;

## cleanup
SELECT 1;
