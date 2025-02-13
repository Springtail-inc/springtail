## repeat 2000 times
BEGIN TRANSACTION;
INSERT INTO benchmark_data (value) VALUES (gen_random_bytes(16));
COMMIT;
## endrepeat
