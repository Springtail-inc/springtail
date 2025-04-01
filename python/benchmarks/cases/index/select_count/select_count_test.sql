DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    FOR i IN 1..2 LOOP
        EXECUTE format('select count(*) from index_test_data') INTO v;
    END LOOP;
END;
$FN$;

