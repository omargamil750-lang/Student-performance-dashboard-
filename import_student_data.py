import psycopg2
import pandas as pd
from datetime import datetime
import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

def create_connection():
    """Create a connection to PostgreSQL database"""
    max_retries = 3
    retry_delay = 1  # seconds
    engine = None

    for attempt in range(max_retries):
        try:
            engine = create_engine(
                'postgresql+psycopg2://postgres:1234@localhost/student_performance',
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    'connect_timeout': 10,
                    'keepalives': 1,
                    'keepalives_idle': 30,
                    'keepalives_interval': 10,
                    'keepalives_count': 5
                }
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Database connection established successfully")
            return engine
            
        except SQLAlchemyError as e:
            if attempt < max_retries - 1:
                print(f"Error connecting to PostgreSQL (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to PostgreSQL after {max_retries} attempts: {e}")
                return None
    
    return engine

def create_tables(engine):
    """Create database tables if they don't exist"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS students (
                    student_id VARCHAR(20) PRIMARY KEY,
                    student_name VARCHAR(100) NOT NULL,
                    gender VARCHAR(10),
                    age INTEGER
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS attendance (
                    attendance_id SERIAL PRIMARY KEY,
                    student_id VARCHAR(20) REFERENCES students(student_id),
                    attendance_rate DECIMAL(5,2),
                    record_date DATE NOT NULL,
                    UNIQUE(student_id, record_date)
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scores (
                    score_id SERIAL PRIMARY KEY,
                    student_id VARCHAR(20),
                    subject_id VARCHAR(20),
                    score DECIMAL(5,2),
                    record_date DATE NOT NULL,
                    UNIQUE(student_id, subject_id, record_date)
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS performance (
                    performance_id SERIAL PRIMARY KEY,
                    student_id VARCHAR(20),
                    performance_score DECIMAL(5,2),
                    overall_grade VARCHAR(10),
                    passed BOOLEAN,
                    record_date DATE NOT NULL,
                    UNIQUE(student_id, record_date)
                )
            """))
            print("Database schema verified/created successfully")
            return True
    except Exception as e:
        print(f"Error creating database schema: {e}")
        return False

def import_student_data(csv_file):
    """Import student data from CSV file into the database"""
            # تنظيف البيانات قبل الإدخال
        df = df.drop_duplicates(subset=['student_id', 'record_date'])  # إزالة التكرارات
        df = df.dropna(subset=['student_id', 'student_name', 'record_date'])  # إزالة القيم الفاضية الأساسية
        df['record_date'] = pd.to_datetime(df['record_date'], errors='coerce')  # تحويل التاريخ
        df = df.dropna(subset=['record_date'])  # إزالة أي صفوف بتاريخ غير صالح
        df['age'] = pd.to_numeric(df['age'], errors='coerce').astype('Int64')
        score_columns = ['math_score', 'reading_score', 'writing_score', 'science_score', 'history_score', 'geography_score']
        for col in score_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
    engine = None
    try:
        engine = create_engine(
            'postgresql+psycopg2://postgres:1234@localhost/student_performance',
            pool_pre_ping=True,
            pool_recycle=3600
        )
        print(f"Reading data from {csv_file}...")
        df = pd.read_csv(csv_file)
        print(f"Successfully read {len(df)} rows from {csv_file}")
        
        df.columns = df.columns.str.strip()
        df['age'] = pd.to_numeric(df['age'], errors='coerce').astype('Int64')
        current_date = datetime.now().strftime('%Y-%m-%d')

        # Import students
        with engine.connect() as conn:
            with conn.begin():
                existing_students = pd.read_sql("SELECT student_id FROM students", conn)
                existing_ids = set(existing_students['student_id']) if not existing_students.empty else set()
                new_students = df[~df['student_id'].isin(existing_ids)]
                if not new_students.empty:
                    students_to_import = new_students[['student_id', 'student_name', 'gender', 'age']].drop_duplicates()
                    try:
                        students_to_import.to_sql('students', conn, if_exists='append', index=False)
                        print(f"Imported {len(students_to_import)} new students")
                    except IntegrityError:
                        print("Some students already existed, skipping duplicates")
                else:
                    print("No new students to import")

        # Import attendance
        with engine.connect() as conn:
            with conn.begin():
                existing_attendance = pd.read_sql("""
                    SELECT student_id, date(record_date) as record_date 
                    FROM attendance 
                    WHERE date(record_date) = CURRENT_DATE
                """, conn)
                attendance = df[['student_id', 'attendance_rate']].copy()
                attendance['record_date'] = current_date
                attendance['key'] = attendance['student_id'] + attendance['record_date'].astype(str)
                existing_attendance['key'] = existing_attendance['student_id'] + existing_attendance['record_date'].astype(str)
                new_attendance = attendance[~attendance['key'].isin(existing_attendance['key'])]
                if not new_attendance.empty:
                    try:
                        new_attendance[['student_id', 'attendance_rate', 'record_date']].to_sql(
                            'attendance', conn, if_exists='append', index=False)
                        print(f"Imported attendance for {len(new_attendance)} new students")
                    except IntegrityError:
                        print("Some attendance records already existed, skipping duplicates")
                else:
                    print("No new attendance records to import")

        # Import scores (مصحح لمنع duplicates)
        with engine.connect() as conn:
            with conn.begin():
                score_columns = {
                    'math_score': 'Math',
                    'reading_score': 'Reading',
                    'writing_score': 'Writing',
                    'science_score': 'Science',
                    'history_score': 'History',
                    'geography_score': 'Geography'
                }
                existing_scores = pd.read_sql("""
                    SELECT student_id, subject_id, record_date
                    FROM scores
                    WHERE date(record_date) = %(current_date)s
                """, conn, params={'current_date': current_date})

                subjects_df = pd.read_sql("SELECT subject_id, subject_name FROM subjects", conn)
                subject_map = dict(zip(subjects_df['subject_name'], subjects_df['subject_id']))

                scores_data = []
                for score_col, subject_name in score_columns.items():
                    if score_col in df.columns:
                        subject_scores = df[['student_id', score_col]].copy()
                        subject_scores['subject_id'] = subject_map[subject_name]
                        subject_scores['record_date'] = current_date
                        scores_data.append(subject_scores.rename(columns={score_col: 'score'}))

                if scores_data:
                    all_scores = pd.concat(scores_data, ignore_index=True)
                    all_scores['record_date'] = pd.to_datetime(all_scores['record_date'])
                    existing_scores['record_date'] = pd.to_datetime(existing_scores['record_date'])
                    merged = all_scores.merge(
                        existing_scores,
                        on=['student_id', 'subject_id', 'record_date'],
                        how='left',
                        indicator=True
                    )
                    new_scores = merged[merged['_merge'] == 'left_only']
                    new_scores = new_scores[['student_id', 'subject_id', 'score', 'record_date']]

                    if not new_scores.empty:
                        new_scores.to_sql('scores', conn, if_exists='append', index=False)
                        print(f"Imported {len(new_scores)} new score records")
                    else:
                        print("No new score records to import")

                # Import scores safely
        with engine.connect() as conn:
            with conn.begin():
                score_columns = {
                    'math_score': 'Math',
                    'reading_score': 'Reading',
                    'writing_score': 'Writing',
                    'science_score': 'Science',
                    'history_score': 'History',
                    'geography_score': 'Geography'
                }

                # جلب subject_ids
                subjects_df = pd.read_sql("SELECT subject_id, subject_name FROM subjects", conn)
                subject_map = dict(zip(subjects_df['subject_name'], subjects_df['subject_id']))

                scores_data = []
                for score_col, subject_name in score_columns.items():
                    if score_col in df.columns:
                        temp = df[['student_id', score_col]].copy()
                        temp['subject_id'] = subject_map[subject_name]
                        temp['score'] = temp[score_col]
                        temp['record_date'] = pd.to_datetime(current_date)
                        scores_data.append(temp[['student_id', 'subject_id', 'score', 'record_date']])

                if scores_data:
                    all_scores = pd.concat(scores_data, ignore_index=True)
                    all_scores = all_scores.dropna(subset=['score'])  # إزالة أي قيم فارغة

                    # جلب السجلات الموجودة مسبقًا
                    existing_scores = pd.read_sql("""
                        SELECT student_id, subject_id, record_date
                        FROM scores
                        WHERE date(record_date) = %(current_date)s
                    """, conn, params={'current_date': current_date})
                    existing_scores['record_date'] = pd.to_datetime(existing_scores['record_date'])

                    # إزالة duplicates
                    merged = all_scores.merge(
                        existing_scores,
                        on=['student_id', 'subject_id', 'record_date'],
                        how='left',
                        indicator=True
                    )
                    new_scores = merged[merged['_merge'] == 'left_only']
                    new_scores = new_scores[['student_id', 'subject_id', 'score', 'record_date']]

                    if not new_scores.empty:
                        new_scores.to_sql('scores', conn, if_exists='append', index=False)
                        print(f"Imported {len(new_scores)} new score records")
                    else:
                        print("No new score records to import")



        print("Data import completed successfully")
        return True

    except Exception as e:
        print(f"Error during data import: {e}")
        return False
    finally:
        if engine is not None:
            engine.dispose()
            print("Database connection closed")

def main():
    csv_file = "student_performance.csv"
    print(f"Current directory: {os.getcwd()}")
    print(f"Files in directory: {os.listdir()}")
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found in {os.getcwd()}")
        return

    print(f"Found {csv_file}, checking contents...")
    engine = create_connection()
    if engine is not None:
        try:
            create_tables(engine)
            import_student_data(csv_file)
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':
    main()
