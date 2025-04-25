DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('select count(*) from customers') INTO v;
END;
$FN$;
