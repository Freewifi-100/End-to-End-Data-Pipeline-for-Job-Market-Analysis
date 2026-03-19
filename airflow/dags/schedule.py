import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime
from datetime import datetime as py_datetime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

@dag(
    dag_id="job_market_data_pipeline",
    # start_date=datetime(2026, 3, 19, tz="Asia/Bangkok"),
    # schedule=CronTriggerTimetable("0 20 * * *", timezone="Asia/Bangkok"),
    # is_paused_upon_creation=False,
    # catchup=False,
)

def job_market_data_pipeline():
    
    @task.python
    def scrape_linkedin(**kwargs):
        from scraper.linkedin_scrape import scrape
        ti = kwargs['ti']
        df = scrape()

        now = py_datetime.now()
        timestamp = now.strftime("%Y-%m-%dT%H%M%S")
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

    scrape_linkedin() >> upload_to_databricks()


job_market_data_pipeline()