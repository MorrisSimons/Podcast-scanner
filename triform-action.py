from typing import Any, Mapping, NotRequired, Optional, Sequence, TypedDict
import triform
import requests
import boto3
from boto3.s3.transfer import TransferConfig
import os
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed


class Inputs(TypedDict):
    batches: list[list[dict[str, Any]]]  # List of batches, each batch has up to 400 episode dicts

class Outputs(TypedDict):
    success: bool
    episode_id: str
    sample_input: dict[str, Any]
    error_message: str | None



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

# Configure multipart uploads for improved throughput and resilience
transfer_config = TransferConfig(
    multipart_threshold=5 * 1024 * 1024,
    multipart_chunksize=16 * 1024 * 1024,
    max_concurrency=25,
    use_threads=True,
)


def upload_from_url_to_s3(url: str, key: str) -> None:
    resp = requests.get(url, stream=True, timeout=(10, 600))
    resp.raise_for_status()
    resp.raw.decode_content = True
    s3_client.upload_fileobj(resp.raw, bucket_name, key, Config=transfer_config)


def update_episode_status(episode_id: str, supabase_url: str, headers: dict, status: bool) -> None:
    url = f"{supabase_url.rstrip('/')}/rest/v1/episodes?id=eq.{episode_id}"
    data = {"mp3_download_status": status}
    resp = requests.patch(url, json=data, headers=headers, timeout=60)
    if resp.status_code != 204:
        raise RuntimeError(f"Failed to update episode {episode_id}: HTTP {resp.status_code} - {resp.text}")


def process_episode(row: dict, supabase_url: str, headers: dict) -> str:
    """Process a single episode: download and mark as complete."""
    audio_url = row.get("audio_url")
    if not audio_url:
        return None
    
    episode_id = row.get("id")
    podcast_id = row.get("podcast_id") or "unknown"
    filename = Path(urlparse(audio_url).path).name or "audio.mp3"
    key = f"{podcast_id}/{filename}"
    
    print(f"Uploading {audio_url} -> s3://{bucket_name}/{key}")

    success = True
    try:
        upload_from_url_to_s3(audio_url, key)
    except Exception as e:
        print(f"Upload failed for episode {episode_id}: {e}")
        success = False

    try:
        update_episode_status(episode_id, supabase_url, headers, success)
    except Exception as e:
        print(f"Status update failed for episode {episode_id}: {e}")

    if success:
        print(f"Episode {episode_id} marked as downloaded.")
        return episode_id
    else:
        print(f"Episode {episode_id} marked as NOT downloaded.")
        return None

    


@triform.entrypoint
def main(inputs: Inputs) -> Outputs:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not supabase_url or not supabase_key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY environment variables")
    
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    batches = inputs.get("batches", [])
    total_uploaded = 0
    
    for batch in batches:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_episode, row, supabase_url, headers) for row in batch]
            for future in as_completed(futures):
                episode_id = future.result()
                if episode_id:
                    total_uploaded += 1
        
        print(f"Processed batch, total uploaded so far: {total_uploaded}")
    
    print(f"Successfully uploaded {total_uploaded} episodes to S3.")
    
    return Outputs(
        success=True,
        episode_id="",
        sample_input=batches[0][0] if batches and batches[0] else {},
        error_message=None
    )