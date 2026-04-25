from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
import io
import os
from pathlib import Path

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_API_KEY")

local_files = "./analyse/data/"

#load table data from databricks
def load_table_data():
    if not host or not token:
        raise ValueError(
            "Missing Databricks credentials. Set DATABRICKS_HOST and DATABRICKS_API_KEY in your environment or .env file."
        )

    w = WorkspaceClient(host=host, token=token)

    volume_dir = "/Volumes/dbt_job_market/default/linkedin_data_volume"

    remote_entries = list(w.files.list_directory_contents(volume_dir))
    if not remote_entries:
        print("Remote volume is empty. No files to download.")
        return

    for entry in remote_entries:
        destination_path = f"{local_files}/{entry.name}"
        with open(destination_path, "wb") as f:
            w.files.download(f"{volume_dir}/{entry.name}", f)
        print(f"Downloaded {entry.name} -> {destination_path}")
