from airflow import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable
import os
from database.upload import upload
from scraper.linkedin_scrape import scrape

@dag(
    dag_id="job_market_data_pipeline",
    start_date=datetime(2026, 3, 19),
    schedule=CronTriggerTimetable("0 20 * * *"),  # Daily at 8 PM
    is_paused_upon_creation=False
    catchup=False,
)

def job_market_data_pipeline():

    @task.python
    def scrape_linkedin(**kwargs):
        ti = kwargs['ti']
        
        scrape()

    @task.python
    def upload_to_databricks(**kwargs):
        from database.upload import upload
        upload()

    scrape_linkedin() >> upload_to_databricks()
    
job_market_data_pipeline()