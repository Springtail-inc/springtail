## test

## verify

### switch_db partitions
SELECT * FROM preload_sales_hash_p0 ORDER BY id;
SELECT * FROM preload_sales_hash_p1 ORDER BY id;
SELECT * FROM preload_sales_hash_p2 ORDER BY id;
SELECT * FROM preload_sales_hash_p3 ORDER BY id;

-- this statement does not work yet
-- SELECT * FROM preload_sales_hash ORDER BY id;

-- these statement does not work yet
-- SELECT * FROM country_sales ORDER BY id;
-- SELECT * FROM country_sales_us ORDER BY id;
-- SELECT * FROM country_sales_ca ORDER BY id;

SELECT * FROM country_sales_us_2023 ORDER BY id;
SELECT * FROM country_sales_us_2024 ORDER BY id;
SELECT * FROM country_sales_ca_2023 ORDER BY id;
SELECT * FROM country_sales_ca_2024 ORDER BY id;


## cleanup

