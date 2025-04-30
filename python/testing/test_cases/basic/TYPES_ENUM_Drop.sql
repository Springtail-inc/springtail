## test
CREATE TYPE colors AS ENUM ('red', 'green', 'blue');
DROP TYPE colors;

CREATE TYPE numbers AS ENUM ('one', 'two', 'three');
CREATE TABLE nums (
    id SERIAL PRIMARY KEY,
    number numbers
);

INSERT INTO nums (number) VALUES ('one'), ('two'), ('three');

DROP TYPE numbers CASCADE;

## verify
SELECT typname, typtype, string_agg(enumlabel, ', ' ORDER BY enumsortorder) AS enum_labels
FROM pg_type
JOIN pg_enum ON pg_enum.enumtypid = pg_type.oid
WHERE typname = 'colors'
AND typtype = 'e'
GROUP BY typname, typtype;

SELECT * FROM nums ORDER BY id;

## cleanup
DROP TYPE IF EXISTS colors;
DROP TABLE IF EXISTS nums;
DROP TYPE IF EXISTS numbers;