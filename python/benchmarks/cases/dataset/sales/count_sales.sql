DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('select count(*) from sales') INTO v;
END;
$FN$;
