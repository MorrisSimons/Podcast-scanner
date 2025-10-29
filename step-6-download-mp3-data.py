import sys
from pathlib import Path
from urllib.parse import urlparse

import requests
# TODO: Use this as an sample when you are done: https://sphinx.acast.com/rmm/intro/media.mp3

# Source MP3 URL
MP3_URL = "https://sphinx.acast.com/p/acast/s/rmm/e/68fc911b8a5d09ce06c3d486/media.mp3"


import boto3
from dotenv import load_dotenv
import os

# Load environment variables from .env if present
load_dotenv()

# Create a session using Scaleway's S3-compatible endpoint
session = boto3.session.Session()

region_name = os.getenv("S3_REGION")
endpoint_url = os.getenv("S3_ENDPOINT_URL")
aws_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")

if not all([region_name, endpoint_url, aws_access_key_id, aws_secret_access_key]):
    raise ValueError(
        "Missing required environment variables: S3_REGION, S3_ENDPOINT_URL, "
        "S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY"
    )

s3_client = session.client(
    service_name='s3',
    region_name=region_name,
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)



def download_mp3(url: str, destination: Path) -> None:
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    with destination.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


def main() -> None:
    project_root = Path(__file__).resolve().parent
    filename = Path(urlparse(MP3_URL).path).name or "download.mp3"
    dest_path = project_root / filename
    print(f"Downloading {MP3_URL} -> {dest_path.name}")
    download_mp3(MP3_URL, dest_path)
    print(f"Saved {dest_path} ({dest_path.stat().st_size} bytes)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)