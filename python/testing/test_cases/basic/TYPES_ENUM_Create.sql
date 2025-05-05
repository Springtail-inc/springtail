## test
CREATE TYPE colors AS ENUM ('red', 'green', 'blue');

## verify
SELECT typname, typtype, string_agg(enumlabel, ', ' ORDER BY enumsortorder) AS enum_labels
FROM pg_type
JOIN pg_enum ON pg_enum.enumtypid = pg_type.oid
WHERE typname = 'colors'
AND typtype = 'e'
GROUP BY typname, typtype;

## cleanup
DROP TYPE IF EXISTS colors;