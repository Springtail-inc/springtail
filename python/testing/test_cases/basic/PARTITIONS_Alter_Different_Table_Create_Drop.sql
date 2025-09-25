## test
CREATE TABLE alter_cascade_in_txn_level1 (
    id INT,
    name TEXT,
    role TEXT,
    subscription TEXT,
    region TEXT,
    department TEXT,
    dummy TEXT
) PARTITION BY LIST (subscription);

CREATE TABLE alter_cascade_in_txn_level2 PARTITION OF alter_cascade_in_txn_level1 FOR VALUES IN ('Paid')
PARTITION BY LIST (role);

CREATE TABLE alter_cascade_in_txn_level2a PARTITION OF alter_cascade_in_txn_level2 FOR VALUES IN ('Admin') PARTITION BY LIST (department);
CREATE TABLE alter_cascade_in_txn_level2b PARTITION OF alter_cascade_in_txn_level2 FOR VALUES IN ('User') PARTITION BY LIST (department);
CREATE TABLE alter_cascade_in_txn_level2c PARTITION OF alter_cascade_in_txn_level2 FOR VALUES IN ('Guest') PARTITION BY LIST (department);

CREATE TABLE alter_cascade_in_txn_level0 (
    id INT,
    name TEXT,
    role TEXT,
    subscription TEXT,
    region TEXT,
    department TEXT,
    dummy TEXT
) PARTITION BY LIST (region);

CREATE TABLE alter_cascade_in_txn_level3a PARTITION OF alter_cascade_in_txn_level2a FOR VALUES IN ('HR');
CREATE TABLE alter_cascade_in_txn_level3b PARTITION OF alter_cascade_in_txn_level2a FOR VALUES IN ('IT');
CREATE TABLE alter_cascade_in_txn_level3c PARTITION OF alter_cascade_in_txn_level2a FOR VALUES IN ('FIN');

CREATE TABLE alter_cascade_in_txn_level3d PARTITION OF alter_cascade_in_txn_level2b FOR VALUES IN ('HR');
CREATE TABLE alter_cascade_in_txn_level3e PARTITION OF alter_cascade_in_txn_level2b FOR VALUES IN ('IT');
CREATE TABLE alter_cascade_in_txn_level3f PARTITION OF alter_cascade_in_txn_level2b FOR VALUES IN ('FIN');

CREATE TABLE alter_cascade_in_txn_level3g PARTITION OF alter_cascade_in_txn_level2c FOR VALUES IN ('HR');
CREATE TABLE alter_cascade_in_txn_level3h PARTITION OF alter_cascade_in_txn_level2c FOR VALUES IN ('IT');
CREATE TABLE alter_cascade_in_txn_level3i PARTITION OF alter_cascade_in_txn_level2c FOR VALUES IN ('FIN');

ALTER TABLE alter_cascade_in_txn_level0 ATTACH PARTITION alter_cascade_in_txn_level1 FOR VALUES IN ('US');

BEGIN;
ALTER TABLE alter_cascade_in_txn_level0 DROP COLUMN dummy;

CREATE TABLE dummy_0 (
    id INT,
    name TEXT,
    role TEXT,
    subscription TEXT,
    region TEXT,
    department TEXT,
    dummy TEXT
);
DROP TABLE dummy_0;
COMMIT;

-- Insert via alter_cascade_in_txn_level1 (routes to L2 by subscription and then by role)
INSERT INTO alter_cascade_in_txn_level1 (id, name, role, subscription, department, region) VALUES
(101, 'L1 Admin A', 'Admin', 'Paid', 'HR', 'US'),
(102, 'L1 User A',  'User',  'Paid', 'HR', 'US'),
(103, 'L1 Guest A', 'Guest', 'Paid', 'HR', 'US'),
(104, 'L1 Admin B', 'Admin', 'Paid', 'HR', 'US'),
(105, 'L1 User B',  'User',  'Paid', 'HR', 'US');

-- Insert via alter_cascade_in_txn_level2 (routes to subpartitions by role)
INSERT INTO alter_cascade_in_txn_level2 (id, name, role, subscription, department, region) VALUES
(1,   'Via L2 Admin 1', 'Admin', 'Paid', 'HR', 'US'),
(2,   'Via L2 User 1',  'User',  'Paid', 'HR', 'US'),
(106, 'Via L2 Guest 1', 'Guest','Paid', 'IT', 'US'),
(107, 'Via L2 Admin 2', 'Admin', 'Paid', 'IT', 'US'),
(108, 'Via L2 User 2',  'User',  'Paid', 'IT', 'US');

-- Insert directly into alter_cascade_in_txn_level2a (Admin)
INSERT INTO alter_cascade_in_txn_level2a (id, name, role, subscription, department, region) VALUES
(3,   'Admin Direct 1', 'Admin', 'Paid', 'HR', 'US'),
(4,   'Admin Direct 2', 'Admin', 'Paid', 'HR', 'US'),
(109, 'Admin Direct 3', 'Admin', 'Paid', 'HR', 'US'),
(110, 'Admin Direct 4', 'Admin', 'Paid', 'IT', 'US'),
(111, 'Admin Direct 5', 'Admin', 'Paid', 'FIN', 'US');

-- Insert directly into alter_cascade_in_txn_level2b (User)
INSERT INTO alter_cascade_in_txn_level2b (id, name, role, subscription, department, region) VALUES
(5,   'User Direct 1', 'User', 'Paid', 'HR', 'US'),
(6,   'User Direct 2', 'User', 'Paid', 'HR', 'US'),
(112, 'User Direct 3', 'User', 'Paid', 'IT', 'US'),
(113, 'User Direct 4', 'User', 'Paid', 'FIN', 'US'),
(114, 'User Direct 5', 'User', 'Paid', 'FIN', 'US');

-- Insert directly into alter_cascade_in_txn_level2c (Guest)
INSERT INTO alter_cascade_in_txn_level2c (id, name, role, subscription, department, region) VALUES
(7,   'Guest Direct 1', 'Guest', 'Paid', 'HR', 'US'),
(115, 'Guest Direct 2', 'Guest', 'Paid', 'IT', 'US'),
(116, 'Guest Direct 3', 'Guest', 'Paid', 'FIN', 'US'),
(117, 'Guest Direct 4', 'Guest', 'Paid', 'FIN', 'US'),
(118, 'Guest Direct 5', 'Guest', 'Paid', 'FIN', 'US');

## verify
### schema_check public alter_cascade_in_txn_level0
### schema_check public alter_cascade_in_txn_level1
### schema_check public alter_cascade_in_txn_level2
### schema_check public alter_cascade_in_txn_level2a
### schema_check public alter_cascade_in_txn_level2b
### schema_check public alter_cascade_in_txn_level2c
### schema_check public alter_cascade_in_txn_level3a
### schema_check public alter_cascade_in_txn_level3b
### schema_check public alter_cascade_in_txn_level3c
### schema_check public alter_cascade_in_txn_level3d
### schema_check public alter_cascade_in_txn_level3e
### schema_check public alter_cascade_in_txn_level3f
### schema_check public alter_cascade_in_txn_level3g
### schema_check public alter_cascade_in_txn_level3h
### schema_check public alter_cascade_in_txn_level3i
### table_exists public dummy_0 false

SELECT * FROM alter_cascade_in_txn_level0 ORDER BY id;

## cleanup
DROP TABLE alter_cascade_in_txn_level0 CASCADE;
