-- Test Case 5: Deleting a Duplicate Row from a Table Without Primary Key
CREATE TABLE IF NOT EXISTS test_delete_duplicates (
    id SERIAL,
    value TEXT
);

INSERT INTO test_delete_duplicates (value) VALUES ('duplicate');
INSERT INTO test_delete_duplicates (value) VALUES ('duplicate');

DELETE FROM test_delete_duplicates WHERE id = 1;

SELECT * FROM test_delete_duplicates;

-- Cleanup
DROP TABLE IF EXISTS test_delete_duplicates;
