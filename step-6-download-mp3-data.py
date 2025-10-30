import sys
import os
from dotenv import load_dotenv
import requests

load_dotenv()


def fetch_episodes() -> list[dict]:
    """Fetch episodes with audio URLs from Supabase (max 1000)"""
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars")
    
    base = SUPABASE_URL.rstrip("/") + "/rest/v1/episodes"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    
    # Fetch up to 1000 (PostgREST default max per request)
    params = {
        "select": "id,podcast_id,audio_url",
        "audio_url": "is.not_null",
        "or": "(mp3_download_status.is.null,mp3_download_status.is.false)",
        "order": "pub_date.desc.nullslast",
        "limit": "250"
    }
    r = requests.get(base, headers=headers, params=params, timeout=90)
    if r.status_code != 200:
        raise RuntimeError(f"Supabase episodes fetch failed: HTTP {r.status_code} - {r.text}")
    
    return r.json() or []


#def mark_episodes_in_progress(episodes: list[dict]) -> None:
#    """Mark fetched episodes as in-progress (mp3_download_status: true)"""
#    SUPABASE_URL = os.getenv("SUPABASE_URL")
#    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
#    
#    base = SUPABASE_URL.rstrip("/") + "/rest/v1/episodes"
#    headers = {
#        "apikey": SUPABASE_SERVICE_ROLE_KEY,
#        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
#    }
#    
#    for episode in episodes:
#        episode_id = episode.get("id")
#        if not episode_id:
#            continue
#        
#        url = f"{base}?id=eq.{episode_id}"
#        data = {"mp3_download_status": True}
#        resp = requests.patch(url, json=data, headers=headers, timeout=60)
#        if resp.status_code != 204:
#            raise RuntimeError(f"Failed to mark episode {episode_id} in-progress: HTTP {resp.status_code} - {resp.text}")


def create_batches(episodes: list[dict], batches_count: int = 25, items_per_batch: int = 10) -> list[list[dict]]:
    """Create batches with specified structure: 20 batches with 25 episodes each"""
    batch_size = items_per_batch
    batches = []
    
    for i in range(0, len(episodes), batch_size):
        batch = episodes[i:i + batch_size]
        batches.append(batch)
    
    return batches


def send_to_triform(batches: list[list[list[dict]]]):
    """Send batches to Triform API"""
    import os
    import json

    api_url = os.getenv("TRIFORM_SLAVE_ENDPOINT")
    ingress_token = os.getenv("TRIFORM_INGRESSTOKEN") or os.getenv("TRIFROM-INGRESSTOKEN")
    if not ingress_token:
        raise RuntimeError("Missing TRIFORM_INGRESSTOKEN environment variable")
    headers = {
        "Authorization": ingress_token,
        "Content-Type": "application/json"
    }
    payload = {"batches": batches}
    print(json.dumps(payload, indent=2))
    
    # Save payload to JSON file
    with open("payload.json", "w") as f:
        json.dump(payload, f, indent=2)
    print("Payload saved to payload.json")
    
    response = requests.post(api_url, json=payload, headers=headers, timeout=60)
    if response.status_code not in [200, 201, 202]:
        raise RuntimeError(f"Triform API request failed: HTTP {response.status_code} - {response.text}")
    
    print(f"Successfully sent {len(batches)} batches to Triform")
    return response


def main() -> None:
    episodes = fetch_episodes()
    print(f"Fetched {len(episodes)} episodes from Supabase")

    batches = create_batches(episodes)
    print(f"Created {len(batches)} batches")
    
    response = send_to_triform(batches)
    print(f"Response: {response.text}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)