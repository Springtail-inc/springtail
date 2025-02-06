DO $$
DECLARE
    i INT;
    random_text BYTEA;
BEGIN
    FOR i IN 1..100000 LOOP
        INSERT INTO benchmark_data (id, value)
        VALUES (i, gen_random_bytes(16));
    END LOOP;
END;
$$;
