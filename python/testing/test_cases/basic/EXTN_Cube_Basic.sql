## test

CREATE TABLE mixed_data (
    id SERIAL PRIMARY KEY,
    dimensions cube,
    properties hstore,
    created_at timestamptz DEFAULT now()
);

-- Main test data
INSERT INTO mixed_data (dimensions, properties)
VALUES
    (
        '(1,1,1)'::cube,
        'city => "Bangalore", population => "8M", famous_for => "IT"'::hstore
    ),
    (
        '(1,2,3)'::cube,
        'city => "Chennai", population => "8M", famous_for => "Beach"'::hstore
    ),
    (
        '(2,2,2)'::cube,
        'city => "Delhi", population => "8M", famous_for => "India Gate"'::hstore
    ),
    (
        '(2,3,4)'::cube,
        'city => "Hyderabad", population => "8M", famous_for => "Charminar"'::hstore
    ),
    (
        '(3,3,3)'::cube,
        'city => "Kolkata", population => "8M", famous_for => "Howrah Bridge"'::hstore
    ),
    (
        '(3,4,5)'::cube,
        'city => "Mumbai", population => "880k", famous_for => "Gateway of India"'::hstore
    );

INSERT INTO mixed_data (dimensions, properties)
VALUES
    -- Test data for negative coordinates
    (
        '(-1,-1,-1)'::cube,
        'city => "Negative Point", population => "0", famous_for => "Testing"'::hstore
    ),
    -- Test data for decimal coordinates
    (
        '(1.5,2.5,3.5)'::cube,
        'city => "Decimal Point", population => "0", famous_for => "Precision Testing"'::hstore
    ),
    -- Test data for NULL values
    (NULL, NULL),
    -- Test data for empty cube and hstore
    ('()'::cube, ''::hstore);

CREATE INDEX idx_cube ON mixed_data (dimensions);

## verify

-- Basic comparison operators
SELECT 'Basic comparison operators' as test_case;

-- Test with points on different axes
SELECT * FROM mixed_data WHERE dimensions > '(2,3,4)' ORDER BY dimensions;
SELECT * FROM mixed_data WHERE dimensions > '(1,1,1)' ORDER BY dimensions;
SELECT * FROM mixed_data WHERE dimensions > '(3,3,3)' ORDER BY dimensions;

SELECT * FROM mixed_data WHERE dimensions < '(2,3,4)' ORDER BY dimensions;
SELECT * FROM mixed_data WHERE dimensions < '(1,2,3)' ORDER BY dimensions;
SELECT * FROM mixed_data WHERE dimensions < '(3,4,5)' ORDER BY dimensions;

-- Test with equal values
SELECT * FROM mixed_data WHERE dimensions = '(2,3,4)' ORDER BY dimensions;
SELECT * FROM mixed_data WHERE dimensions = '(9,9,9)' ORDER BY dimensions;

-- Test with != operator
SELECT * FROM mixed_data WHERE dimensions != '(2,3,4)' ORDER BY dimensions;

-- Test with range conditions
SELECT * FROM mixed_data
WHERE dimensions BETWEEN '(1,2,3)' AND '(3,3,3)'
ORDER BY dimensions;

-- Test with points on the boundary
SELECT * FROM mixed_data
WHERE dimensions >= '(2,3,4)'
ORDER BY dimensions;

SELECT * FROM mixed_data
WHERE dimensions <= '(2,3,4)'
ORDER BY dimensions;

-- Test with negative coordinates
SELECT * FROM mixed_data
WHERE dimensions < '(0,0,0)' ORDER BY dimensions;

-- Test with decimal values
SELECT 'Decimal coordinates' as test_operator, * FROM mixed_data
WHERE dimensions > '(1.4,2.4,3.4)' AND dimensions < '(1.6,2.6,3.6)';

-- Cube specific functions
-- Test cube_distance
SELECT
    a.city,
    b.city,
    cube_distance(
        a.dimensions,
        b.dimensions
    ) as distance
FROM
    (SELECT properties->'city' as city, dimensions FROM mixed_data) a,
    (SELECT properties->'city' as city, dimensions FROM mixed_data) b
WHERE
    a.city < b.city
ORDER BY
    distance, a.city, b.city
LIMIT 5;

-- Test cube_dim
SELECT properties->'city' as city, cube_dim(dimensions) as dimensions
FROM mixed_data
ORDER BY city;

-- Test cube_contains
SELECT
    a.properties->'city' as container,
    b.properties->'city' as contained
FROM
    mixed_data a,
    mixed_data b
WHERE
    a.dimensions @> b.dimensions
    AND a.properties->'city' != b.properties->'city';

-- Edge cases
-- Test with NULL values
SELECT * FROM mixed_data WHERE dimensions IS NULL;
-- Test with empty cube
SELECT * FROM mixed_data WHERE dimensions = '()'::cube;

## cleanup

DROP TABLE IF EXISTS mixed_data;
