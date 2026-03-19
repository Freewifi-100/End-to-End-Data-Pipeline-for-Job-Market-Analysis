from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
import io
import os
from pathlib import Path

load_dotenv()

def upload(local_files) -> None:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_API_KEY")

    if not host or not token:
        raise ValueError(
            "Missing Databricks credentials. Set DATABRICKS_HOST and DATABRICKS_API_KEY in your environment or .env file."
        )

    w = WorkspaceClient(host=host, token=token)

    volume_dir = "/Volumes/dbt_job_market/default/linkedin_data_volume"

    if not local_files:
        raise FileNotFoundError("No input files provided for upload")

    # Normalize to a list of Path objects.
    normalized_files = [Path(p) for p in local_files]
    missing_files = [p for p in normalized_files if not p.exists()]
    if missing_files:
        raise FileNotFoundError(f"Input files not found: {missing_files}")

    for local_file_path in normalized_files:
        destination_path = f"{volume_dir}/{local_file_path.name}"
        with local_file_path.open("rb") as f:
            w.files.upload(destination_path, io.BytesIO(f.read()), overwrite=True)
        print(f"Uploaded {local_file_path} -> {destination_path}")


if __name__ == "__main__":
    upload(sorted(Path("./raw/linkedin").glob("*.json")))

