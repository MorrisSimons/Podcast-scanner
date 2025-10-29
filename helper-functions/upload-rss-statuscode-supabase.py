import os
import json
from pathlib import Path
import requests
from dotenv import load_dotenv


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    output_dir = project_root / "temp_rss_output"
    if not output_dir.exists():
        raise FileNotFoundError(f"temp_rss_output not found: {output_dir}")

    load_dotenv(project_root / ".env")
    supabase_url = os.getenv("SUPABASE_URL")
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    rest_url = supabase_url.rstrip("/") + "/rest/v1/podcast_profiles"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
    }

    # Collect IDs from filenames like <id>_rss.xml
    ids = []
    for file_path in output_dir.glob("*_rss.xml"):
        stem = file_path.stem  # e.g., "<id>_rss"
        if stem.endswith("_rss"):
            ids.append(stem[:-4])
    if not ids:
        print("No *_rss.xml files found. Nothing to update.")
        return

    print(f"Found {len(ids)} successful RSS files. Updating status_code=200 in Supabase...")

    # Update each id individually (simple and reliable)
    updated = 0
    for podcast_id in ids:
        resp = requests.patch(
            rest_url,
            headers=headers,
            params={"id": f"eq.{podcast_id}"},
            data=json.dumps({"RSS_request_status_code": 200}),
            timeout=30,
        )
        if resp.status_code not in (200, 204):
            raise RuntimeError(
                f"Failed to update id={podcast_id}: HTTP {resp.status_code} - {resp.text}"
            )
        updated += 1

    print(f"Updated status_code=200 for {updated} rows.")


if __name__ == "__main__":
    main()


