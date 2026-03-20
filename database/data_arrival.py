from pathlib import Path
from databricks import sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from databricks import sql
import os

load_dotenv()
host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_API_KEY")
warehouse_id = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")

def check_data_arrival(latest_json: Path) -> None:
        if not host or not token:
                raise ValueError("Missing DATABRICKS_HOST or DATABRICKS_API_KEY")
        if not warehouse_id:
                raise ValueError("Missing DATABRICKS_SQL_WAREHOUSE_ID")


        connection = sql.connect(
                                server_hostname = host,
                                http_path = warehouse_id,
                                access_token = token)

        cursor = connection.cursor()

        cursor.execute("""select scraped_at, count(*) as rows from dbt_job_market.default.linkedin_data
                        group by scraped_at
                        order by scraped_at desc limit 10""")

        rows = cursor.fetchall()
        loaded_scraped_at = rows[0][0]
        loaded_row_count = rows[0][1]
        print(f"Latest scraped_at in table: {loaded_scraped_at}, row count: {loaded_row_count}")

        with latest_json.open("r", encoding="utf-8") as f:
                file_row_count = sum(1 for line in f if line.strip())

        print(f"latest scraped_at in table: {loaded_scraped_at}")
        print(f"loaded row count in table: {loaded_row_count}")
        print(f"latest local file: {latest_json.name}")
        print(f"row count in latest local file: {file_row_count}")

        if loaded_row_count == file_row_count:
                print("MATCH: table row count equals raw/linkedin file row count")
                status = 'success'
        else:
                print("MISMATCH: table row count does not equal raw/linkedin file row count")
                status = 'failure'

        cursor.close()
        connection.close()
        return status

if __name__ == "__main__":
        raw_dir = Path(__file__).resolve().parents[1] / "raw" / "linkedin"
        latest_json = max(raw_dir.glob("*.json"), key=lambda p: p.stat().st_mtime)
        # print(f"Checking data arrival for latest file: {latest_json}")
        status = check_data_arrival(latest_json)
        print(f"Data arrival check status: {status}")