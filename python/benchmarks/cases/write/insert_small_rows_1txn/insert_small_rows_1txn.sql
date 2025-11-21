BEGIN TRANSACTION;

## repeat 100000 times
INSERT INTO benchmark_data (value) VALUES (gen_random_bytes(16));
INSERT INTO benchmark_data_with_index (value)
VALUES (floor(random() * 10000)::int);
## endrepeat

COMMIT;
