DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    FOR i IN 1..10000 LOOP
        EXECUTE format('select * from index_test_data where col2=42') INTO v;
    END LOOP;
END;
$FN$;

