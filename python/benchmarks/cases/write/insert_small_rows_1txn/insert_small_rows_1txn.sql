BEGIN TRANSACTION;

## repeat 100000 times
INSERT INTO benchmark_data (value) VALUES (gen_random_bytes(16));
## endrepeat

COMMIT;
