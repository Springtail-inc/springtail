CREATE TABLE IF NOT EXISTS test3 (
    id SERIAL PRIMARY KEY,
    value TEXT
);

INSERT INTO test3 (value) VALUES ('delete_me');

DELETE FROM test3 WHERE value = 'delete_me';

SELECT * FROM test3 WHERE value = 'delete_me';

