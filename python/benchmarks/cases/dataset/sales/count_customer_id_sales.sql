DO $FN$
DECLARE
    i INT;
    v record;
BEGIN
    EXECUTE format('select count(*) from sales where customerid=20') INTO v;
END;
$FN$;
