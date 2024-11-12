## test
-- Create a table to store the large dataset.
CREATE TABLE IF NOT EXISTS student_data_large (
    id SERIAL PRIMARY KEY,
    Hours_Studied INT,
    Attendance INT,
    Parental_Involvement TEXT,
    Access_to_Resources TEXT,
    Extracurricular_Activities TEXT,
    Sleep_Hours INT,
    Previous_Scores INT,
    Motivation_Level TEXT,
    Internet_Access TEXT,
    Tutoring_Sessions INT,
    Family_Income TEXT,
    Teacher_Quality TEXT,
    School_Type TEXT,
    Peer_Influence TEXT,
    Physical_Activity INT,
    Learning_Disabilities TEXT,
    Parental_Education_Level TEXT,
    Distance_from_Home TEXT,
    Gender TEXT,
    Exam_Score INT
);

-- Load data from the 1M rows CSV file.
COPY student_data_large(Hours_Studied, Attendance, Parental_Involvement, Access_to_Resources, 
                        Extracurricular_Activities, Sleep_Hours, Previous_Scores, Motivation_Level, 
                        Internet_Access, Tutoring_Sessions, Family_Income, Teacher_Quality, School_Type, 
                        Peer_Influence, Physical_Activity, Learning_Disabilities, Parental_Education_Level, 
                        Distance_from_Home, Gender, Exam_Score) 
FROM '/path/to/1M_dataset.csv' DELIMITER ',' CSV HEADER;

-- Count the rows to ensure all data was loaded.
SELECT COUNT(*) AS row_count FROM student_data_large;

## verify
-- Verify row count is consistent between primary and replica databases.
SELECT COUNT(*) AS row_count FROM student_data_large;

## cleanup
-- Drop the table after the test.
DROP TABLE IF EXISTS student_data_large;
