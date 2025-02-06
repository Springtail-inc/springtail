DO $$
DECLARE
    i INT;
    random_text BYTEA;
BEGIN
    FOR i IN 1..10000 LOOP
        INSERT INTO benchmark_data (id, value)
        VALUES (i, encrypt(repeat(E'c', 102400)::bytea, LPAD(i::text, 16, '0')::bytea, 'aes'));
    END LOOP;
END;
$$;
