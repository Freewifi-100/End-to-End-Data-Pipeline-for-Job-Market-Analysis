import sys
from pathlib import Path
import os
import subprocess
import time
from airflow.decorators import dag, task
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime
from datetime import datetime, timezone, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}project/dbt_job_market"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/opt/airflow/venv/bin/dbt"

@dag(
    dag_id="job_market_data_pipeline",
    # start_date=datetime(2026, 3, 19, tz="Asia/Bangkok"),
    # schedule=CronTriggerTimetable("30 3 * * *", timezone="Asia/Bangkok"),
    is_paused_upon_creation=True,
    catchup=False,
)

def job_market_data_pipeline():
    
    @task.python
    def scrape_linkedin(**kwargs):
        from scraper.linkedin_scrape import scrape
        ti = kwargs['ti']
        df = scrape()

        tz_utc_plus_7 = timezone(timedelta(hours=7))
        now_utc_plus_7 = datetime.now(timezone.utc).astimezone(tz_utc_plus_7)
        timestamp = now_utc_plus_7.strftime("%Y-%m-%dT%H%M%S")
        output_dir = Path("/opt/airflow/project/raw/linkedin")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{timestamp}.json"
        df.to_json(output_path.as_posix(), orient="records", lines=True)
        ti.xcom_push(key="latest_file", value=output_path.as_posix())
        

    @task.python
    def upload_to_databricks(**kwargs):
        from database.databricks_upload import upload

        ti = kwargs['ti']
        latest_file = ti.xcom_pull(key="latest_file", task_ids="scrape_linkedin")
        print(f"Pulled latest file path from XCom: {latest_file}")

        if not latest_file:
            raise ValueError("No file path found in XCom key 'latest_file' from scrape_linkedin")

        latest_path = Path(latest_file)
        if not latest_path.exists():
            raise FileNotFoundError(f"Scraped file not found: {latest_path}")

        print(f"File {latest_path} has content. Proceeding to upload.")
        upload([latest_path])
        ti.xcom_push(key="uploaded_file", value=latest_path.as_posix())
        time.sleep(120)

    

    @task.python
    def check_data_arrival(**kwargs):
        from database.data_arrival import check_data_arrival
         # Initial delay before starting to check for data arrival

        ti = kwargs['ti']
        latest_file = ti.xcom_pull(key="uploaded_file", task_ids="upload_to_databricks")
        print(f"Pulled latest file path from XCom for data arrival check: {latest_file}")

        if not latest_file:
            raise ValueError("No file path found in XCom key 'latest_file' from scrape_linkedin")

        latest_path = Path(latest_file)
        if not latest_path.exists():
            raise FileNotFoundError(f"Scraped file not found for data arrival check: {latest_path}")

        print(f"Checking data arrival for file: {latest_path}")
        status = check_data_arrival(latest_path)
        ti.xcom_push(key="data_arrival_checked_file", value=status)
        
    
    @task.python
    def start_pipeline(**kwargs):
        print("Starting job market data pipeline...")
        ti = kwargs['ti']
        status = ti.xcom_pull(key="data_arrival_checked_file", task_ids="check_data_arrival")
        if status == 'success':
            print("Data arrival check completed successfully.")
            subprocess.run(["dbt", "build"], check=True)
        else:
            raise ValueError("Data arrival check did not complete successfully.")



        

    scrape_linkedin() >> upload_to_databricks() >> check_data_arrival() >> start_pipeline()


job_market_data_pipeline()