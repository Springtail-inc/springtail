## repeat 2000 times
BEGIN TRANSACTION;
INSERT INTO benchmark_data (value)
VALUES (encrypt(repeat(E'c', 102400)::bytea, gen_random_bytes(16), 'aes'));
COMMIT;
## endrepeat
