import requests
import json
import os
from pathlib import Path
from dotenv import load_dotenv
import time
from tqdm import tqdm

# Load environment
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

# Supabase REST config
REST_URL = SUPABASE_URL.rstrip("/") + "/rest/v1/podcast_profiles"
HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
}

# Ensure output directory exists
output_dir = Path("temp_rss_output")
output_dir.mkdir(parents=True, exist_ok=True)

# Start: get data from Supabase
# Fetch all rows (paginate)
rows = []
page_size = 1000
start = 0
print("Fetching podcast_profiles from Supabase...")
while True:
    time.sleep(3)
    resp = requests.get(
        REST_URL,
        headers=HEADERS,
        params={"select": "id,podcast_name,rss_feed_url", "limit": page_size, "offset": start},
        timeout=60,
    )
    if resp.status_code not in (200, 206):
        raise RuntimeError(f"Failed to fetch podcast_profiles: HTTP {resp.status_code} - {resp.text}")
    batch = resp.json()
    if not batch:
        break
    print(f"Fetched {len(batch)} rows (total: {len(rows) + len(batch)})")
    rows.extend(batch)
    if len(batch) < page_size:
        break
    start += page_size

print(f"Fetched {len(rows)} rows from Supabase.")

# Start: process each row
print("Starting RSS downloads for fetched rows...")
for row in tqdm(rows, desc="RSS downloads", unit="feed"):
    rss_url = row.get("rss_feed_url")
    if not rss_url:
        print(f"No rss_feed_url found for id={row.get('id')}")
        continue

    podcast_name = row.get("podcast_name") or row.get("id")
    # Log which podcast we're fetching
    print(f"Fetching RSS feed for: {podcast_name}")

    time.sleep(5)
    response = requests.get(rss_url, timeout=60)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch RSS for id={row.get('id')}: HTTP {response.status_code}")

    # Save the RSS feed response (use id to ensure uniqueness)
    output_file = f"{row.get('id')}_rss.xml"
    with open(output_dir / output_file, "w", encoding="utf-8") as f:
        f.write(response.text)

    print(f"Saved to {output_file}")
