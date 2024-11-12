## test
-- Ensure the large dataset table exists (run this after inserting large data).
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

## test
-- Insert additional rows to simulate increased data load.
INSERT INTO student_data_large (Hours_Studied, Attendance, Parental_Involvement, Access_to_Resources, 
                                Extracurricular_Activities, Sleep_Hours, Previous_Scores, Motivation_Level, 
                                Internet_Access, Tutoring_Sessions, Family_Income, Teacher_Quality, School_Type, 
                                Peer_Influence, Physical_Activity, Learning_Disabilities, Parental_Education_Level, 
                                Distance_from_Home, Gender, Exam_Score)
VALUES
    (25, 90, 'High', 'High', 'Yes', 8, 80, 'High', 'Yes', 2, 'High', 'High', 'Private', 'Positive', 3, 'No', 'High School', 'Near', 'Female', 85),
    (18, 70, 'Medium', 'Medium', 'No', 6, 65, 'Medium', 'No', 0, 'Medium', 'Medium', 'Public', 'Neutral', 4, 'No', 'College', 'Moderate', 'Male', 72);

## verify
-- Verify new row count in both primary and replica databases.
SELECT COUNT(*) AS row_count FROM student_data_large;

## cleanup
-- Drop the table after the test.
DROP TABLE IF EXISTS student_data_large;
