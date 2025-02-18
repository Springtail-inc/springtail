DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    FOR i IN 1..100000 LOOP
        EXECUTE format('select * from index_test_data order by col2') INTO v;
    END LOOP;
END;
$FN$;

