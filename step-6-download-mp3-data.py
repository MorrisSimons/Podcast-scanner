import sys
import os
from pathlib import Path
from urllib.parse import urlparse

import requests
import boto3
from dotenv import load_dotenv

load_dotenv()

# Initialize S3 client
session = boto3.session.Session()
region_name = os.getenv("S3_REGION")
endpoint_url = os.getenv("S3_ENDPOINT_URL")
aws_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
bucket_name = os.getenv("S3_BUCKET")

if not all([region_name, endpoint_url, aws_access_key_id, aws_secret_access_key, bucket_name]):
    raise ValueError(
        "Missing required environment variables: S3_REGION, S3_ENDPOINT_URL, "
        "S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET"
    )

if "/" in region_name:
    raise ValueError(f"Invalid S3_REGION '{region_name}'. Use hyphen format like 'pl-waw'.")

s3_client = session.client(
    service_name='s3',
    region_name=region_name,
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)


def upload_from_url_to_s3(url: str, key: str) -> None:
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    resp.raw.decode_content = True
    s3_client.upload_fileobj(resp.raw, bucket_name, key)


def main() -> None:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars")
    
    base = SUPABASE_URL.rstrip("/") + "/rest/v1/episodes"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    params = {
        "select": "podcast_id,audio_url,title",
        "audio_url": "is.not_null",
        "order": "pub_date.desc.nullslast",
        "limit": "3",
    }
    
    r = requests.get(base, headers=headers, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Supabase episodes fetch failed: HTTP {r.status_code} - {r.text}")
    
    rows = r.json()
    if not rows:
        raise RuntimeError("No episodes with non-null audio_url found in database")
    
    for row in rows:
        audio_url = row.get("audio_url")
        if not audio_url:
            continue
        podcast_id = row.get("podcast_id") or "unknown"
        filename = Path(urlparse(audio_url).path).name or "audio.mp3"
        key = f"{podcast_id}/{filename}"
        print(f"Uploading {audio_url} -> s3://{bucket_name}/{key}")
        upload_from_url_to_s3(audio_url, key)
    
    print(f"Successfully uploaded {len(rows)} episodes to S3.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)