import sys
from pathlib import Path
from urllib.parse import urlparse

import requests
# TODO: Use this as an sample when you are done: https://sphinx.acast.com/rmm/intro/media.mp3

# Source MP3 URL
MP3_URL = "https://sphinx.acast.com/p/acast/s/rmm/e/68fc911b8a5d09ce06c3d486/media.mp3"


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