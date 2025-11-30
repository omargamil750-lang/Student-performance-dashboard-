-- Database schema for Student Performance System

-- Students table to store basic student information
CREATE TABLE IF NOT EXISTS students (
    student_id VARCHAR(10) PRIMARY KEY,
    student_name VARCHAR(100) NOT NULL,
    gender VARCHAR(10),
    age INTEGER  -- This allows NULL values
);

-- Subjects table
CREATE TABLE IF NOT EXISTS subjects (
    subject_id SERIAL PRIMARY KEY,
    subject_name VARCHAR(50) UNIQUE NOT NULL
);

-- Insert core subjects
INSERT INTO subjects (subject_name) VALUES 
    ('Math'),
    ('Reading'),
    ('Writing'),
    ('Science'),
    ('History'),
    ('Geography')
ON CONFLICT (subject_name) DO NOTHING;

-- Student attendance table
CREATE TABLE IF NOT EXISTS attendance (
    attendance_id SERIAL PRIMARY KEY,
    student_id VARCHAR(10) REFERENCES students(student_id),
    attendance_rate DECIMAL(5,2),
    record_date DATE DEFAULT CURRENT_DATE,
    UNIQUE(student_id, record_date)
);

-- Student scores table
CREATE TABLE IF NOT EXISTS scores (
    score_id SERIAL PRIMARY KEY,
    student_id VARCHAR(10) REFERENCES students(student_id),
    subject_id INTEGER REFERENCES subjects(subject_id),
    score DECIMAL(5,2),
    record_date DATE DEFAULT CURRENT_DATE,
    UNIQUE(student_id, subject_id, record_date)
);

-- Student performance summary
CREATE TABLE IF NOT EXISTS performance (
    performance_id SERIAL PRIMARY KEY,
    student_id VARCHAR(10) REFERENCES students(student_id),
    performance_score DECIMAL(5,2),
    overall_grade VARCHAR(2),
    passed BOOLEAN,
    record_date DATE DEFAULT CURRENT_DATE,
    UNIQUE(student_id, record_date)
);

-- Indexes for better query performance
CREATE INDEX idx_scores_student_id ON scores(student_id);
CREATE INDEX idx_scores_subject_id ON scores(subject_id);
CREATE INDEX idx_attendance_student_id ON attendance(student_id);
CREATE INDEX idx_performance_student_id ON performance(student_id);
