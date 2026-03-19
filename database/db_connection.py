import psycopg2
import csv
import os
from datetime import datetime
from pathlib import Path

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5435")
DB_NAME = os.getenv("DB_NAME", "job_market_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secret")

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database successfully!")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def insert_job_post(file_name):
    conn = connect_to_db()
    if conn is None:
        raise RuntimeError("Database connection failed")
    cur = conn.cursor()

    project_root = Path(__file__).resolve().parents[1]
    linkedin_dir = project_root / "raw" /  "linkedin"

    if file_name.endswith(".csv"):
        matching_files = [linkedin_dir / file_name]
    else:
        matching_files = sorted(linkedin_dir.glob(f"{file_name}*.csv"))

    if not matching_files:
        raise FileNotFoundError(
            f"No CSV file found for '{file_name}' in {linkedin_dir}"
        )

    csv_path = matching_files[-1]

    with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
        data_reader = csv.reader(csvfile)
        next(data_reader)  # Skip header row
        for row in data_reader:
            cur.execute("""
                INSERT INTO job_postings (job_id, job_website, search_keyword, job_url, job_title, company_name, company_url, 
                        time_posted, num_applicants, location, seniority_level, employment_type, job_function, industries, job_description)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", row)

    conn.commit()
    cur.close()
    conn.close()    
    print(f"Job post inserted successfully from {csv_path.name}!")

def run_job_import():
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d")
    file_name = os.getenv("JOB_FILE_NAME", f"{timestamp}")
    insert_job_post(file_name)

if __name__ == "__main__":
    run_job_import()