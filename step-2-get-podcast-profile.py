import os
import json
from pathlib import Path
import time
import random

import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
  raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

REST_URL = SUPABASE_URL.rstrip("/") + "/rest/v1/podcast_profiles"
HEADERS = {
  "apikey": SUPABASE_SERVICE_ROLE_KEY,
  "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
  "Content-Type": "application/json",
  "Prefer": "resolution=merge-duplicates,return=representation",
}

podcasts_file = Path("podcasts.json")
if not podcasts_file.exists():
  raise FileNotFoundError("podcasts.json not found")

id_to_name = json.loads(podcasts_file.read_text(encoding="utf-8"))

for podcast_id, podcast_name in list(id_to_name.items()):
  url = f"https://api.mediafacts.se/api/podcast/v1/podcasts/details?id={podcast_id}&fromweek=43&fromyear=2025"
  resp = requests.get(url, timeout=30)
  status_code = resp.status_code
  data = resp.json() if status_code == 200 else {}

  row = {
    "id": podcast_id,
    "rss_feed_url": data.get("rssFeedUrl"),
    "podcast_name": data.get("podcastName"),
    "supplier_id": data.get("supplierId"),
    "supplier_name": data.get("supplierName"),
    "network_id": data.get("networkId"),
    "network_name": data.get("networkName"),
    "genre": data.get("genre"),
    "status_code": status_code,
  }

  r = requests.post(REST_URL, headers=HEADERS, params={"on_conflict": "id"}, data=json.dumps(row))
  if r.status_code not in (200, 201):
    raise RuntimeError(f"Upsert failed for id={podcast_id}: HTTP {r.status_code} - {r.text}")

  print(f"id={podcast_id} name={podcast_name} status={status_code}")

  if status_code != 200:
    delay = random.uniform(15, 30)
    print(f"Waiting {delay:.1f}s...")
    time.sleep(delay)
