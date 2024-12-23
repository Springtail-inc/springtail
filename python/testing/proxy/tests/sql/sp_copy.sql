CREATE TABLE main_table (a int unique, b int);

-- test copy from stdin
COPY main_table (a,b) FROM stdin DELIMITER ',';
5,10
20,20
30,10
50,35
80,15
\.

SELECT * from main_table ORDER BY a DESC;

-- test copy to stdout
COPY main_table (a,b) TO stdout DELIMITER ',';

