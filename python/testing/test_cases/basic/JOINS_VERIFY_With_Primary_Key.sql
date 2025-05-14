## test

CREATE TABLE IF NOT EXISTS departments (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    job_title TEXT,
    salary NUMERIC(10, 2),
    hire_date DATE,
    department_id INTEGER REFERENCES departments(id),
    manager_id INTEGER REFERENCES employees(id)
);

-- Insert departments with employees
INSERT INTO departments (id, name) VALUES
    (1, 'Engineering'),
    (2, 'Human Resources'),
    (3, 'Marketing'),
    (4, 'Sales'),
    (5, 'Finance');

-- Insert departments without employees
INSERT INTO departments (id, name) VALUES
    (6, 'Research & Development'),
    (7, 'Customer Support'),
    (8, 'Legal'),
    (9, 'Operations'),
    (10, 'Information Technology'),
    (11, 'Quality Assurance'),
    (12, 'Product Management'),
    (13, 'Public Relations'),
    (14, 'Business Development'),
    (15, 'Supply Chain');


-- Insert managers
INSERT INTO employees (id, name, job_title, salary, hire_date, department_id, manager_id) VALUES
    (1, 'Manager 1', 'Department Manager', 92000.00, '2011-01-01', 2, NULL),
    (2, 'Manager 2', 'Department Manager', 94000.00, '2012-01-01', 3, NULL),
    (3, 'Manager 3', 'Department Manager', 96000.00, '2013-01-01', 4, NULL),
    (4, 'Manager 4', 'Department Manager', 98000.00, '2014-01-01', 5, NULL),
    (5, 'Manager 5', 'Department Manager', 90000.00, '2015-01-01', 1, NULL),
    (6, 'Manager 6', 'Department Manager', 92000.00, '2016-01-01', 2, NULL),
    (7, 'Manager 7', 'Department Manager', 94000.00, '2017-01-01', 3, NULL),
    (8, 'Manager 8', 'Department Manager', 96000.00, '2018-01-01', 4, NULL),
    (9, 'Manager 9', 'Department Manager', 98000.00, '2019-01-01', 5, NULL),
    (10, 'Manager 10', 'Department Manager', 90000.00, '2010-01-01', 1, NULL),
    (11, 'Manager 11', 'Department Manager', 92000.00, '2011-01-01', 2, NULL),
    (12, 'Manager 12', 'Department Manager', 94000.00, '2012-01-01', 3, NULL),
    (13, 'Manager 13', 'Department Manager', 96000.00, '2013-01-01', 4, NULL),
    (14, 'Manager 14', 'Department Manager', 98000.00, '2014-01-01', 5, NULL),
    (15, 'Manager 15', 'Department Manager', 90000.00, '2015-01-01', 1, NULL),
    (16, 'Manager 16', 'Department Manager', 92000.00, '2016-01-01', 2, NULL),
    (17, 'Manager 17', 'Department Manager', 94000.00, '2017-01-01', 3, NULL),
    (18, 'Manager 18', 'Department Manager', 96000.00, '2018-01-01', 4, NULL),
    (19, 'Manager 19', 'Department Manager', 98000.00, '2019-01-01', 5, NULL),
    (20, 'Manager 20', 'Department Manager', 90000.00, '2010-01-01', 1, NULL);

INSERT INTO employees (id, name, job_title, salary, hire_date, department_id, manager_id) VALUES
    (21, 'Employee 21', 'Employee', 51500.00, '2021-06-22', 2, 2),
    (22, 'Employee 22', 'Employee', 53000.00, '2022-06-23', 3, 3),
    (23, 'Employee 23', 'Employee', 54500.00, '2023-06-24', 4, 4),
    (24, 'Employee 24', 'Employee', 56000.00, '2024-06-25', 5, 5),
    (25, 'Employee 25', 'Employee', 57500.00, '2020-06-26', 1, 6),
    (26, 'Employee 26', 'Employee', 59000.00, '2021-06-27', 2, 7),
    (27, 'Employee 27', 'Employee', 60500.00, '2022-06-28', 3, 8),
    (28, 'Employee 28', 'Employee', 62000.00, '2023-06-01', 4, 9),
    (29, 'Employee 29', 'Employee', 63500.00, '2024-06-02', 5, 10),
    (30, 'Employee 30', 'Employee', 65000.00, '2020-06-03', 1, 11),
    (31, 'Employee 31', 'Employee', 66500.00, '2021-06-04', 2, 12),
    (32, 'Employee 32', 'Employee', 68000.00, '2022-06-05', 3, 13),
    (33, 'Employee 33', 'Employee', 69500.00, '2023-06-06', 4, 14),
    (34, 'Employee 34', 'Employee', 71000.00, '2024-06-07', 5, 15),
    (35, 'Employee 35', 'Employee', 72500.00, '2020-06-08', 1, 16),
    (36, 'Employee 36', 'Employee', 74000.00, '2021-06-09', 2, 17),
    (37, 'Employee 37', 'Employee', 75500.00, '2022-06-10', 3, 18),
    (38, 'Employee 38', 'Employee', 77000.00, '2023-06-11', 4, 19),
    (39, 'Employee 39', 'Employee', 78500.00, '2024-06-12', 5, 20),
    (40, 'Employee 40', 'Employee', 50000.00, '2020-06-13', 1, 1),
    (41, 'Employee 41', 'Employee', 51500.00, '2021-06-14', 2, 2),
    (42, 'Employee 42', 'Employee', 53000.00, '2022-06-15', 3, 3),
    (43, 'Employee 43', 'Employee', 54500.00, '2023-06-16', 4, 4),
    (44, 'Employee 44', 'Employee', 56000.00, '2024-06-17', 5, 5),
    (45, 'Employee 45', 'Employee', 57500.00, '2020-06-18', 1, 6),
    (46, 'Employee 46', 'Employee', 59000.00, '2021-06-19', 2, 7),
    (47, 'Employee 47', 'Employee', 60500.00, '2022-06-20', 3, 8),
    (48, 'Employee 48', 'Employee', 62000.00, '2023-06-21', 4, 9),
    (49, 'Employee 49', 'Employee', 63500.00, '2024-06-22', 5, 10),
    (50, 'Employee 50', 'Employee', 65000.00, '2020-06-23', 1, 11),
    (51, 'Employee 51', 'Employee', 66500.00, '2021-06-24', 2, 12),
    (52, 'Employee 52', 'Employee', 68000.00, '2022-06-25', 3, 13),
    (53, 'Employee 53', 'Employee', 69500.00, '2023-06-26', 4, 14),
    (54, 'Employee 54', 'Employee', 71000.00, '2024-06-27', 5, 15),
    (55, 'Employee 55', 'Employee', 72500.00, '2020-06-28', 1, 16),
    (56, 'Employee 56', 'Employee', 74000.00, '2021-06-01', 2, 17),
    (57, 'Employee 57', 'Employee', 75500.00, '2022-06-02', 3, 18),
    (58, 'Employee 58', 'Employee', 77000.00, '2023-06-03', 4, 19),
    (59, 'Employee 59', 'Employee', 78500.00, '2024-06-04', 5, 20),
    (60, 'Employee 60', 'Employee', 50000.00, '2020-06-05', 1, 1),
    (61, 'Employee 61', 'Employee', 51500.00, '2021-06-06', 2, 2),
    (62, 'Employee 62', 'Employee', 53000.00, '2022-06-07', 3, 3),
    (63, 'Employee 63', 'Employee', 54500.00, '2023-06-08', 4, 4),
    (64, 'Employee 64', 'Employee', 56000.00, '2024-06-09', 5, 5),
    (65, 'Employee 65', 'Employee', 57500.00, '2020-06-10', 1, 6),
    (66, 'Employee 66', 'Employee', 59000.00, '2021-06-11', 2, 7),
    (67, 'Employee 67', 'Employee', 60500.00, '2022-06-12', 3, 8),
    (68, 'Employee 68', 'Employee', 62000.00, '2023-06-13', 4, 9),
    (69, 'Employee 69', 'Employee', 63500.00, '2024-06-14', 5, 10),
    (70, 'Employee 70', 'Employee', 65000.00, '2020-06-15', 1, 11),
    (71, 'Employee 71', 'Employee', 66500.00, '2021-06-16', 2, 12),
    (72, 'Employee 72', 'Employee', 68000.00, '2022-06-17', 3, 13),
    (73, 'Employee 73', 'Employee', 69500.00, '2023-06-18', 4, 14),
    (74, 'Employee 74', 'Employee', 71000.00, '2024-06-19', 5, 15),
    (75, 'Employee 75', 'Employee', 72500.00, '2020-06-20', 1, 16),
    (76, 'Employee 76', 'Employee', 74000.00, '2021-06-21', 2, 17),
    (77, 'Employee 77', 'Employee', 75500.00, '2022-06-22', 3, 18),
    (78, 'Employee 78', 'Employee', 77000.00, '2023-06-23', 4, 19),
    (79, 'Employee 79', 'Employee', 78500.00, '2024-06-24', 5, 20),
    (80, 'Employee 80', 'Employee', 50000.00, '2020-06-25', 1, 1),
    (81, 'Employee 81', 'Employee', 51500.00, '2021-06-26', 2, 2),
    (82, 'Employee 82', 'Employee', 53000.00, '2022-06-27', 3, 3),
    (83, 'Employee 83', 'Employee', 54500.00, '2023-06-28', 4, 4),
    (84, 'Employee 84', 'Employee', 56000.00, '2024-06-01', 5, 5),
    (85, 'Employee 85', 'Employee', 57500.00, '2020-06-02', 1, 6),
    (86, 'Employee 86', 'Employee', 59000.00, '2021-06-03', 2, 7),
    (87, 'Employee 87', 'Employee', 60500.00, '2022-06-04', 3, 8),
    (88, 'Employee 88', 'Employee', 62000.00, '2023-06-05', 4, 9),
    (89, 'Employee 89', 'Employee', 63500.00, '2024-06-06', 5, 10),
    (90, 'Employee 90', 'Employee', 65000.00, '2020-06-07', 1, 11),
    (91, 'Employee 91', 'Employee', 66500.00, '2021-06-08', 2, 12),
    (92, 'Employee 92', 'Employee', 68000.00, '2022-06-09', 3, 13),
    (93, 'Employee 93', 'Employee', 69500.00, '2023-06-10', 4, 14),
    (94, 'Employee 94', 'Employee', 71000.00, '2024-06-11', 5, 15),
    (95, 'Employee 95', 'Employee', 72500.00, '2020-06-12', 1, 16),
    (96, 'Employee 96', 'Employee', 74000.00, '2021-06-13', 2, 17),
    (97, 'Employee 97', 'Employee', 75500.00, '2022-06-14', 3, 18),
    (98, 'Employee 98', 'Employee', 77000.00, '2023-06-15', 4, 19),
    (99, 'Employee 99', 'Employee', 78500.00, '2024-06-16', 5, 20),
    (100, 'Employee 100', 'Employee', 50000.00, '2020-06-17', 1, 1),
    (101, 'Employee 101', 'Employee', 51500.00, '2021-06-18', 2, 2),
    (102, 'Employee 102', 'Employee', 53000.00, '2022-06-19', 3, 3),
    (103, 'Employee 103', 'Employee', 54500.00, '2023-06-20', 4, 4),
    (104, 'Employee 104', 'Employee', 56000.00, '2024-06-21', 5, 5),
    (105, 'Employee 105', 'Employee', 57500.00, '2020-06-22', 1, 6),
    (106, 'Employee 106', 'Employee', 59000.00, '2021-06-23', 2, 7),
    (107, 'Employee 107', 'Employee', 60500.00, '2022-06-24', 3, 8),
    (108, 'Employee 108', 'Employee', 62000.00, '2023-06-25', 4, 9),
    (109, 'Employee 109', 'Employee', 63500.00, '2024-06-26', 5, 10),
    (110, 'Employee 110', 'Employee', 65000.00, '2020-06-27', 1, 11),
    (111, 'Employee 111', 'Employee', 66500.00, '2021-06-28', 2, 12),
    (112, 'Employee 112', 'Employee', 68000.00, '2022-06-01', 3, 13),
    (113, 'Employee 113', 'Employee', 69500.00, '2023-06-02', 4, 14),
    (114, 'Employee 114', 'Employee', 71000.00, '2024-06-03', 5, 15),
    (115, 'Employee 115', 'Employee', 72500.00, '2020-06-04', 1, 16),
    (116, 'Employee 116', 'Employee', 74000.00, '2021-06-05', 2, 17),
    (117, 'Employee 117', 'Employee', 75500.00, '2022-06-06', 3, 18),
    (118, 'Employee 118', 'Employee', 77000.00, '2023-06-07', 4, 19),
    (119, 'Employee 119', 'Employee', 78500.00, '2024-06-08', 5, 20),
    (120, 'Employee 120', 'Employee', 50000.00, '2020-06-09', 1, 1),
    (121, 'Employee 121', 'Employee', 51500.00, '2021-06-10', 2, 2),
    (122, 'Employee 122', 'Employee', 53000.00, '2022-06-11', 3, 3),
    (123, 'Employee 123', 'Employee', 54500.00, '2023-06-12', 4, 4),
    (124, 'Employee 124', 'Employee', 56000.00, '2024-06-13', 5, 5),
    (125, 'Employee 125', 'Employee', 57500.00, '2020-06-14', 1, 6),
    (126, 'Employee 126', 'Employee', 59000.00, '2021-06-15', 2, 7),
    (127, 'Employee 127', 'Employee', 60500.00, '2022-06-16', 3, 8),
    (128, 'Employee 128', 'Employee', 62000.00, '2023-06-17', 4, 9),
    (129, 'Employee 129', 'Employee', 63500.00, '2024-06-18', 5, 10),
    (130, 'Employee 130', 'Employee', 65000.00, '2020-06-19', 1, 11),
    (131, 'Employee 131', 'Employee', 66500.00, '2021-06-20', 2, 12),
    (132, 'Employee 132', 'Employee', 68000.00, '2022-06-21', 3, 13),
    (133, 'Employee 133', 'Employee', 69500.00, '2023-06-22', 4, 14),
    (134, 'Employee 134', 'Employee', 71000.00, '2024-06-23', 5, 15),
    (135, 'Employee 135', 'Employee', 72500.00, '2020-06-24', 1, 16),
    (136, 'Employee 136', 'Employee', 74000.00, '2021-06-25', 2, 17),
    (137, 'Employee 137', 'Employee', 75500.00, '2022-06-26', 3, 18),
    (138, 'Employee 138', 'Employee', 77000.00, '2023-06-27', 4, 19),
    (139, 'Employee 139', 'Employee', 78500.00, '2024-06-28', 5, 20),
    (140, 'Employee 140', 'Employee', 50000.00, '2020-06-01', 1, 1),
    (141, 'Employee 141', 'Employee', 51500.00, '2021-06-02', 2, 2),
    (142, 'Employee 142', 'Employee', 53000.00, '2022-06-03', 3, 3),
    (143, 'Employee 143', 'Employee', 54500.00, '2023-06-04', 4, 4),
    (144, 'Employee 144', 'Employee', 56000.00, '2024-06-05', 5, 5),
    (145, 'Employee 145', 'Employee', 57500.00, '2020-06-06', 1, 6),
    (146, 'Employee 146', 'Employee', 59000.00, '2021-06-07', 2, 7),
    (147, 'Employee 147', 'Employee', 60500.00, '2022-06-08', 3, 8),
    (148, 'Employee 148', 'Employee', 62000.00, '2023-06-09', 4, 9),
    (149, 'Employee 149', 'Employee', 63500.00, '2024-06-10', 5, 10),
    (150, 'Employee 150', 'Employee', 65000.00, '2020-06-11', 1, 11),
    (151, 'Employee 151', 'Employee', 66500.00, '2021-06-12', 2, 12),
    (152, 'Employee 152', 'Employee', 68000.00, '2022-06-13', 3, 13),
    (153, 'Employee 153', 'Employee', 69500.00, '2023-06-14', 4, 14),
    (154, 'Employee 154', 'Employee', 71000.00, '2024-06-15', 5, 15),
    (155, 'Employee 155', 'Employee', 72500.00, '2020-06-16', 1, 16),
    (156, 'Employee 156', 'Employee', 74000.00, '2021-06-17', 2, 17),
    (157, 'Employee 157', 'Employee', 75500.00, '2022-06-18', 3, 18),
    (158, 'Employee 158', 'Employee', 77000.00, '2023-06-19', 4, 19),
    (159, 'Employee 159', 'Employee', 78500.00, '2024-06-20', 5, 20),
    (160, 'Employee 160', 'Employee', 50000.00, '2020-06-21', 1, 1),
    (161, 'Employee 161', 'Employee', 51500.00, '2021-06-22', 2, 2),
    (162, 'Employee 162', 'Employee', 53000.00, '2022-06-23', 3, 3),
    (163, 'Employee 163', 'Employee', 54500.00, '2023-06-24', 4, 4),
    (164, 'Employee 164', 'Employee', 56000.00, '2024-06-25', 5, 5),
    (165, 'Employee 165', 'Employee', 57500.00, '2020-06-26', 1, 6),
    (166, 'Employee 166', 'Employee', 59000.00, '2021-06-27', 2, 7),
    (167, 'Employee 167', 'Employee', 60500.00, '2022-06-28', 3, 8),
    (168, 'Employee 168', 'Employee', 62000.00, '2023-06-01', 4, 9),
    (169, 'Employee 169', 'Employee', 63500.00, '2024-06-02', 5, 10),
    (170, 'Employee 170', 'Employee', 65000.00, '2020-06-03', 1, 11),
    (171, 'Employee 171', 'Employee', 66500.00, '2021-06-04', 2, 12),
    (172, 'Employee 172', 'Employee', 68000.00, '2022-06-05', 3, 13),
    (173, 'Employee 173', 'Employee', 69500.00, '2023-06-06', 4, 14),
    (174, 'Employee 174', 'Employee', 71000.00, '2024-06-07', 5, 15),
    (175, 'Employee 175', 'Employee', 72500.00, '2020-06-08', 1, 16),
    (176, 'Employee 176', 'Employee', 74000.00, '2021-06-09', 2, 17),
    (177, 'Employee 177', 'Employee', 75500.00, '2022-06-10', 3, 18),
    (178, 'Employee 178', 'Employee', 77000.00, '2023-06-11', 4, 19),
    (179, 'Employee 179', 'Employee', 78500.00, '2024-06-12', 5, 20),
    (180, 'Employee 180', 'Employee', 50000.00, '2020-06-13', 1, 1),
    (181, 'Employee 181', 'Employee', 51500.00, '2021-06-14', 2, 2),
    (182, 'Employee 182', 'Employee', 53000.00, '2022-06-15', 3, 3),
    (183, 'Employee 183', 'Employee', 54500.00, '2023-06-16', 4, 4),
    (184, 'Employee 184', 'Employee', 56000.00, '2024-06-17', 5, 5),
    (185, 'Employee 185', 'Employee', 57500.00, '2020-06-18', 1, 6),
    (186, 'Employee 186', 'Employee', 59000.00, '2021-06-19', 2, 7),
    (187, 'Employee 187', 'Employee', 60500.00, '2022-06-20', 3, 8),
    (188, 'Employee 188', 'Employee', 62000.00, '2023-06-21', 4, 9),
    (189, 'Employee 189', 'Employee', 63500.00, '2024-06-22', 5, 10),
    (190, 'Employee 190', 'Employee', 65000.00, '2020-06-23', 1, 11),
    (191, 'Employee 191', 'Employee', 66500.00, '2021-06-24', 2, 12),
    (192, 'Employee 192', 'Employee', 68000.00, '2022-06-25', 3, 13),
    (193, 'Employee 193', 'Employee', 69500.00, '2023-06-26', 4, 14),
    (194, 'Employee 194', 'Employee', 71000.00, '2024-06-27', 5, 15),
    (195, 'Employee 195', 'Employee', 72500.00, '2020-06-28', 1, 16),
    (196, 'Employee 196', 'Employee', 74000.00, '2021-06-01', 2, 17),
    (197, 'Employee 197', 'Employee', 75500.00, '2022-06-02', 3, 18),
    (198, 'Employee 198', 'Employee', 77000.00, '2023-06-03', 4, 19),
    (199, 'Employee 199', 'Employee', 78500.00, '2024-06-04', 5, 20),
    (200, 'Employee 200', 'Employee', 50000.00, '2020-06-05', 1, 1),
    (201, 'Employee 201', 'Employee', 51500.00, '2021-06-06', 2, 2),
    (202, 'Employee 202', 'Employee', 53000.00, '2022-06-07', 3, 3),
    (203, 'Employee 203', 'Employee', 54500.00, '2023-06-08', 4, 4),
    (204, 'Employee 204', 'Employee', 56000.00, '2024-06-09', 5, 5),
    (205, 'Employee 205', 'Employee', 57500.00, '2020-06-10', 1, 6),
    (206, 'Employee 206', 'Employee', 59000.00, '2021-06-11', 2, 7),
    (207, 'Employee 207', 'Employee', 60500.00, '2022-06-12', 3, 8),
    (208, 'Employee 208', 'Employee', 62000.00, '2023-06-13', 4, 9),
    (209, 'Employee 209', 'Employee', 63500.00, '2024-06-14', 5, 10),
    (210, 'Employee 210', 'Employee', 65000.00, '2020-06-15', 1, 11),
    (211, 'Employee 211', 'Employee', 66500.00, '2021-06-16', 2, 12),
    (212, 'Employee 212', 'Employee', 68000.00, '2022-06-17', 3, 13),
    (213, 'Employee 213', 'Employee', 69500.00, '2023-06-18', 4, 14),
    (214, 'Employee 214', 'Employee', 71000.00, '2024-06-19', 5, 15),
    (215, 'Employee 215', 'Employee', 72500.00, '2020-06-20', 1, 16),
    (216, 'Employee 216', 'Employee', 74000.00, '2021-06-21', 2, 17),
    (217, 'Employee 217', 'Employee', 75500.00, '2022-06-22', 3, 18),
    (218, 'Employee 218', 'Employee', 77000.00, '2023-06-23', 4, 19),
    (219, 'Employee 219', 'Employee', 78500.00, '2024-06-24', 5, 20),
    (220, 'Employee 220', 'Employee', 50000.00, '2020-06-25', 1, 1);

INSERT INTO employees (id, name, job_title, salary, hire_date, department_id, manager_id) VALUES
    (221, 'Contractor 1', 'Contractor', 45000.00, '2024-01-01', NULL, 1),
    (222, 'Contractor 2', 'Contractor', 46000.00, '2024-01-02', NULL, 2),
    (223, 'Contractor 3', 'Contractor', 47000.00, '2024-01-03', NULL, 3),
    (224, 'Temp Worker 1', 'Temporary', 40000.00, '2024-01-04', NULL, 4),
    (225, 'Temp Worker 2', 'Temporary', 41000.00, '2024-01-05', NULL, 5);




## verify

-- inner join primary key and foreign key
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    INNER JOIN departments d
        ON e.department_id = d.id
        ORDER BY e.id;

-- left join primary key and foreign key
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    LEFT JOIN departments d
        ON e.department_id = d.id
        ORDER BY e.id;

-- left anti-join primary key and foreign key
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    LEFT JOIN departments d
        ON e.department_id = d.id
        WHERE d.id IS NULL
        ORDER BY e.id;

-- right join primary key and foreign key
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    RIGHT JOIN departments d
        ON e.department_id = d.id
        ORDER BY e.id, d.id;

-- right anti-join primary key and foreign key
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    RIGHT JOIN departments d
        ON e.department_id = d.id
        WHERE e.id IS NULL
        ORDER BY e.id, d.id;

-- full outer join primary key and foreign key
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    FULL OUTER JOIN departments d
        ON e.department_id = d.id
        ORDER BY e.id, d.id;

-- full outer anti-join primary key and foreign key
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    FULL OUTER JOIN departments d
        ON e.department_id = d.id
        WHERE e.id IS NULL OR d.id IS NULL
        ORDER BY e.id, d.id;

-- cross join primary key and foreign key
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    CROSS JOIN departments d
        ORDER BY e.id;

-- self join primary key and foreign key
SELECT
    e1.name AS employee_name,
    e2.name AS manager_name
FROM employees e1
    INNER JOIN employees e2
        ON e1.manager_id = e2.id
        ORDER BY e1.id;

-- natural join primary key and foreign key
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    NATURAL JOIN departments d
    ORDER BY e.id;

-- join primary key and foreign key with using
SELECT
    e.name AS employee_name,
    d.name AS department_name
FROM employees e
    JOIN departments d
        USING (id)
        ORDER BY e.id;



## cleanup

DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS departments CASCADE;