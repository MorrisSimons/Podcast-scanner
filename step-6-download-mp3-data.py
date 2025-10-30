import sys
import os
import csv
import time
from datetime import datetime
from dotenv import load_dotenv
import requests

load_dotenv()


def fetch_episodes() -> list[dict]:
    """Fetch episodes with audio URLs from Supabase (max 300)"""
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars")
    
    base = SUPABASE_URL.rstrip("/") + "/rest/v1/episodes"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    
    # Fetch up to 300 (PostgREST default max per request)
    params = {
        "select": "id,podcast_id,audio_url",
        "audio_url": "is.not_null",
        "or": "(mp3_download_status.is.null)",
        "order": "pub_date.desc.nullslast",
        "limit": "300"
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


def create_batches(episodes: list[dict], batches_count: int = 15, items_per_batch: int = 20) -> list[list[dict]]:
    """Create batches with specified structure: 15 batches with 10 episodes each"""
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
    payload = {"sample_input": batches}
 
   
    response = requests.post(api_url, json=payload, headers=headers)
    if response.status_code not in [200, 201, 202]:
        raise RuntimeError(f"Triform API request failed: HTTP {response.status_code} - {response.text}")
    
    print(f"Successfully sent {len(batches)} batches to Triform")
    return response


def log_to_csv(iteration: int, timestamp: str, episodes_count: int, batches_count: int,
               supabase_duration: float, triform_duration: float,
               status_code: int, response_text: str) -> None:
    """Append iteration metrics to CSV log file"""
    csv_file = "step-6-loop-log.csv"
    file_exists = os.path.isfile(csv_file)
    
    with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        if not file_exists:
            headers = [
                "iteration",
                "timestamp",
                "episodes_count",
                "batches_count",
                "supabase_duration_seconds",
                "triform_duration_seconds",
                "triform_status_code",
                "triform_response_text"
            ]
            writer.writerow(headers)
        
        writer.writerow([
            iteration,
            timestamp,
            episodes_count,
            batches_count,
            round(supabase_duration, 3),
            round(triform_duration, 3),
            status_code,
            response_text
        ])


def main() -> None:
    iteration = 0
    total_episodes_processed = 0
    
    while True:
        iteration += 1
        time.sleep(2)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"\n{'='*60}")
        print(f"ITERATION {iteration} - {timestamp}")
        print(f"{'='*60}")
        
        # Time Supabase fetch
        print(f"[{timestamp}] Fetching episodes from Supabase...")
        supabase_start = time.time()
        episodes = fetch_episodes()
        supabase_duration = time.time() - supabase_start
        print(f"[{timestamp}] Fetched {len(episodes)} episodes from Supabase in {supabase_duration:.3f}s")
        
        if not episodes:
            print(f"\n{'='*60}")
            print(f"COMPLETED - No more episodes to process")
            print(f"Total iterations: {iteration - 1}")
            print(f"Total episodes processed: {total_episodes_processed}")
            print(f"{'='*60}")
            break

        total_episodes_processed += len(episodes)
        
        print(f"[{timestamp}] Creating batches...")
        batches = create_batches(episodes)
        print(f"[{timestamp}] Created {len(batches)} batches with {len(episodes)} episodes")
        
        # Time Triform API call
        print(f"[{timestamp}] Sending to Triform API...")
        triform_start = time.time()
        response = send_to_triform(batches)
        triform_duration = time.time() - triform_start
        print(f"[{timestamp}] Triform API completed in {triform_duration:.3f}s")
        print(f"[{timestamp}] Response status: {response.status_code}")
        print(f"[{timestamp}] Response: {response.text}")
        
        # Log to CSV
        log_to_csv(
            iteration=iteration,
            timestamp=timestamp,
            episodes_count=len(episodes),
            batches_count=len(batches),
            supabase_duration=supabase_duration,
            triform_duration=triform_duration,
            status_code=response.status_code,
            response_text=response.text
        )
        
        print(f"\nIteration {iteration} summary:")
        print(f"  - Episodes in this batch: {len(episodes)}")
        print(f"  - Total episodes processed so far: {total_episodes_processed}")
        print(f"  - Batches created: {len(batches)}")
        print(f"  - Logged to CSV: step-6-loop-log.csv")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)