## repeat 2000 times
BEGIN TRANSACTION;
INSERT INTO benchmark_data (value) VALUES (gen_random_bytes(16));
INSERT INTO benchmark_data_with_index (value)
VALUES (floor(random() * 10000)::int);
COMMIT;
## endrepeat
