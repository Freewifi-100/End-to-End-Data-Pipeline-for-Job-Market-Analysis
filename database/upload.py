from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from datetime import date
import io
import os
from pathlib import Path

load_dotenv()

def upload() -> None:
  host = os.getenv("DATABRICKS_HOST")
  token = os.getenv("DATABRICKS_API_KEY")

  if not host or not token:
    raise ValueError(
      "Missing Databricks credentials. Set DATABRICKS_HOST and DATABRICKS_API_KEY in your environment or .env file."
    )

  w = WorkspaceClient(host=host, token=token)

  # Directory in Unity Catalog volumes where files should be uploaded.
  volume_dir = "/Volumes/dbt_job_market/default/linkedin_data_volume"
  local_files = sorted(Path("./raw/linkedin").glob("*.json"))

  if not local_files:
    raise FileNotFoundError("No JSON files found under ./raw/linkedin")

  remote_entries = list(w.files.list_directory_contents(volume_dir))
  remote_is_empty = len(remote_entries) == 0

  if remote_is_empty:
    files_to_upload = local_files
    print("Remote volume is empty. Uploading all local JSON files.")
  else:
    today_prefix = date.today().isoformat()
    files_to_upload = [p for p in local_files if p.name.startswith(today_prefix)]
    print("Remote volume has files. Uploading only today's JSON files.")

  if not files_to_upload:
    print("No matching JSON files to upload.")
    return

  for local_file_path in files_to_upload:
    destination_path = f"{volume_dir}/{local_file_path.name}"
    with local_file_path.open("rb") as f:
      w.files.upload(destination_path, io.BytesIO(f.read()), overwrite=True)
    print(f"Uploaded {local_file_path} -> {destination_path}")


if __name__ == "__main__":
  upload()
