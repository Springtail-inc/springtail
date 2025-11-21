## repeat 2000 times
BEGIN TRANSACTION;
INSERT INTO benchmark_data (value)
VALUES (encrypt(repeat(E'c', 102400)::bytea, gen_random_bytes(16), 'aes'));
INSERT INTO benchmark_data_with_index (value)
VALUES (floor(random() * 10000)::int);
COMMIT;
## endrepeat
