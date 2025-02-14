DROP TABLE IF EXISTS index_test_data;
CREATE TABLE index_test_data(
    col1 INT PRIMARY KEY,
    col2 INT NOT NULL
);

CREATE INDEX idx_col2 ON index_test_data (col2);
