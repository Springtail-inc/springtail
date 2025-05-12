## test

CREATE TABLE IF NOT EXISTS fruit_basket_one (
    id INT PRIMARY KEY,
    fruit_name VARCHAR (100) NOT NULL
);

CREATE TABLE IF NOT EXISTS fruit_basket_two (
    id INT PRIMARY KEY,
    fruit_name VARCHAR (100) NOT NULL
);

INSERT INTO fruit_basket_one (id, fruit_name) VALUES
    (1, 'apple'),
    (2, 'banana'),
    (3, 'cherry'),
    (4, 'date'),
    (5, 'elderberry'),
    (6, 'fig'),
    (7, 'grape'),
    (8, 'honeydew'),
    (9, 'kiwi'),
    (10, 'lemon'),
    (11, 'mango'),
    (12, 'nectarine'),
    (13, 'orange'),
    (14, 'papaya'),
    (15, 'quince'),
    (16, 'raspberry'),
    (17, 'strawberry'),
    (18, 'tangerine'),
    (19, 'ugli fruit'),
    (20, 'vanilla bean');

INSERT INTO fruit_basket_two(id, fruit_name) VALUES
    (1, 'apple'),
    (2, 'banana'),
    (3, 'cherry'),
    (4, 'dragonfruit'),
    (5, 'elderberry'),
    (6, 'fig'),
    (7, 'guava'),
    (8, 'honeydew'),
    (9, 'jackfruit'),
    (10, 'lemon'),
    (11, 'mango'),
    (12, 'nectarine'),
    (13, 'orange'),
    (14, 'papaya'),
    (15, 'pear'),
    (16, 'quince'),
    (17, 'raspberry'),
    (18, 'starfruit'),
    (19, 'tangerine'),
    (20, 'ugli fruit');

## verify

-- join by unindexed column with using
SELECT
    one.id AS fruit_one,
    one.fruit_name AS fruit_one,
    two.id AS fruit_two,
    two.fruit_name AS fruit_two
FROM fruit_basket_one one
    JOIN fruit_basket_two two
        USING (fruit_name)
        ORDER BY one.id, two.id;

-- inner join by unindexed column
SELECT
    one.id AS fruit_one,
    one.fruit_name AS fruit_one,
    two.id AS fruit_two,
    two.fruit_name AS fruit_two
FROM fruit_basket_one one
    INNER JOIN fruit_basket_two two
        ON one.fruit_name = two.fruit_name
        ORDER BY one.id, two.id;

-- left join by unindexed column
SELECT
    one.id AS fruit_one,
    one.fruit_name AS fruit_one,
    two.id AS fruit_two,
    two.fruit_name AS fruit_two
FROM fruit_basket_one one
    LEFT JOIN fruit_basket_two two
        ON one.fruit_name = two.fruit_name
        ORDER BY one.id, two.id;

-- left anti-join by unindexed column
SELECT
    one.id AS fruit_one,
    one.fruit_name AS fruit_one,
    two.id AS fruit_two,
    two.fruit_name AS fruit_two
FROM fruit_basket_one one
    LEFT JOIN fruit_basket_two two
        ON one.fruit_name = two.fruit_name
        WHERE two.fruit_name IS NULL
        ORDER BY one.id, two.id;

-- right join by unindexed column
SELECT
    one.id AS fruit_one,
    one.fruit_name AS fruit_one,
    two.id AS fruit_two,
    two.fruit_name AS fruit_two
FROM fruit_basket_one one
    RIGHT JOIN fruit_basket_two two
        ON one.fruit_name = two.fruit_name
        ORDER BY one.id, two.id;

-- right anti-join by unindexed column
SELECT
    one.id AS fruit_one,
    one.fruit_name AS fruit_one,
    two.id AS fruit_two,
    two.fruit_name AS fruit_two
FROM fruit_basket_one one
    RIGHT JOIN fruit_basket_two two
        ON one.fruit_name = two.fruit_name
        WHERE one.fruit_name IS NULL
        ORDER BY one.id, two.id;

-- cross join by unindexed column
SELECT
    one.id AS fruit_one,
    one.fruit_name AS fruit_one,
    two.id AS fruit_two,
    two.fruit_name AS fruit_two
FROM fruit_basket_one one
    CROSS JOIN fruit_basket_two two
        ORDER BY one.id, two.id;

-- natural join by unindexed column
SELECT
    one.id AS fruit_one,
    one.fruit_name AS fruit_one,
    two.id AS fruit_two,
    two.fruit_name AS fruit_two
FROM fruit_basket_one one
    NATURAL JOIN fruit_basket_two two
        ORDER BY one.id, two.id;

## cleanup

DROP TABLE IF EXISTS fruit_basket_one CASCADE;
DROP TABLE IF EXISTS fruit_basket_two CASCADE;