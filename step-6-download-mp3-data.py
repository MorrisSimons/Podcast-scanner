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
        "mp3_download_status": "is.null",
        "order": "pub_date.desc.nullslast",
        "limit": "300"
    }
    # Retry on HTTP 500 with exponential backoff
    max_attempts = 5
    r = None
    for attempt in range(1, max_attempts + 1):
        r = requests.get(base, headers=headers, params=params, timeout=90)
        if r.status_code == 500:
            if attempt == max_attempts:
                raise RuntimeError(
                    f"Supabase episodes fetch failed after {max_attempts} attempts: HTTP 500 - {r.text}"
                )
            backoff_seconds = min(60, 2 ** (attempt - 1))
            time.sleep(backoff_seconds)
            continue
        break

    if r.status_code != 200:
        raise RuntimeError(f"Supabase episodes fetch failed: HTTP {r.status_code} - {r.text}")
    
    return r.json() or []


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
    if response.status_code == 504:
        # Gateway timeout - return response to handle in main loop
        return response
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
        
        # Handle 504 timeout
        if response.status_code == 504:
            print(f"[{timestamp}] WARNING: Triform returned 504 Gateway Timeout")
            print(f"[{timestamp}] Sleeping for 30 seconds before retrying...")
            time.sleep(30)
            print(f"[{timestamp}] Retrying Triform API request...")
            response = send_to_triform(batches)
            print(f"[{timestamp}] Retry response status: {response.status_code}")
            print(f"[{timestamp}] Retry response: {response.text}")
            
            # If still 504 after retry, skip this batch #TODO: Handle this better because we dont need to crash here just retry until it wokrs.
            if response.status_code == 504:
                raise RuntimeError("Triform still returning 504 after retry")
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