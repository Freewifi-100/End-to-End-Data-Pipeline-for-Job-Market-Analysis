from scraper.linkedin_scrape import scrape
from datetime import datetime
import os

uri = "./raw/linkedin/"
now = datetime.now()
timestamp = now.strftime("%Y-%m-%dT%H%M%S")

# ti = kwargs['ti']
df = scrape()
os.makedirs(os.path.dirname(uri), exist_ok=True)
with open(f"{uri}{timestamp}.json", "w") as f:
    df.to_json(f, orient="records", lines=True)
print(f"Saved scraped data to {uri}{timestamp}.json")