## test
CREATE SCHEMA IF NOT EXISTS "Test_schema's_1";

CREATE TYPE "Test_schema's_1"."badName" AS ENUM ('red', 'green', 'blue');
CREATE TABLE public.balloons (
    id SERIAL PRIMARY KEY,
    color "Test_schema's_1"."badName"
);
INSERT INTO public.balloons (color) VALUES ('red'), ('green'), ('blue');

ALTER TYPE "Test_schema's_1"."badName" SET SCHEMA public;
ALTER TYPE public."badName" RENAME TO colors;
ALTER TYPE public.colors RENAME VALUE 'red' TO 'yellow';
ALTER TYPE public.colors ADD VALUE 'purple' BEFORE 'yellow';
ALTER TYPE public.colors ADD VALUE 'orange' AFTER 'yellow';
ALTER TYPE public.colors ADD VALUE 'violet' AFTER 'blue';

## verify
SELECT * FROM balloons ORDER BY id;

SELECT typname, typtype, string_agg(enumlabel, ', ' ORDER BY enumsortorder) AS enum_labels
FROM pg_type
JOIN pg_enum ON pg_enum.enumtypid = pg_type.oid
WHERE typname = 'colors'
AND typtype = 'e'
GROUP BY typname, typtype;

## cleanup
DROP TABLE IF EXISTS public.balloons;
DROP TYPE IF EXISTS colors;
DROP SCHEMA IF EXISTS "Test_schema's_1" CASCADE;