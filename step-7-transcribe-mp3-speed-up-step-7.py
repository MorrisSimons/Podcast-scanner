import os
import sys
import argparse
import queue
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

import boto3
from faster_whisper import WhisperModel
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import re
from tqdm import tqdm
import json
import time
import socket
from datetime import timedelta
import traceback

import redis
from redis.exceptions import ResponseError

load_dotenv()

print("SCRIPT: Imports completed")


def make_s3_client():
    print("DEBUG: make_s3_client() called")
    s3 = boto3.session.Session().client(
        service_name="s3",
        region_name=os.getenv("S3_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
    )
    bucket = os.getenv("S3_BUCKET")
    print(f"S3_BUCKET: {bucket}")
    print(f"S3_REGION: {os.getenv('S3_REGION')}")
    print(f"S3_ENDPOINT_URL: {os.getenv('S3_ENDPOINT_URL')}")
    print(f"S3_ACCESS_KEY_ID: {os.getenv('S3_ACCESS_KEY_ID')}")
    print(f"S3_SECRET_ACCESS_KEY: {os.getenv('S3_SECRET_ACCESS_KEY')}")
    if not bucket:
        raise ValueError("S3_BUCKET is required")
    print("DEBUG: make_s3_client() returning")
    return s3, bucket


def list_audio_keys(s3, bucket: str, prefix: Optional[str]) -> List[str]:
    """List audio object keys in S3 that do not yet have a transcript.

    Why: We avoid wasting bandwidth/GPU time by skipping files that already
    have a transcript uploaded next to them in S3.
    """
    # Only consider files with known audio extensions
    #TODO: i dont know the audio files extensions is in the data but my guess is that is only mp3
    audio_suffixes = (".mp3", ".wav", ".m4a", ".ogg", ".flac", ".webm", ".opus") 
    # Use S3 pagination to handle large buckets efficiently
    paginator = s3.get_paginator("list_objects_v2")

    # Build request, optionally scoping to a prefix to limit listing
    request = {"Bucket": bucket}
    if prefix:
        request["Prefix"] = prefix

    # Collect only keys that still need transcription
    pending_keys: List[str] = []
    for page in paginator.paginate(**request):
        contents = page.get("Contents", [])
        for obj in contents:
            # Extract key and filter to audio files only
            key = obj.get("Key")
            if not key or not key.lower().endswith(audio_suffixes):
                continue

            # Derive transcript key and include only if it does not exist in S3
            if not transcript_exists(s3, bucket, transcript_key_for(key)):
                pending_keys.append(key)
    return pending_keys


def transcript_key_for(audio_key: str) -> str:
    p = Path(audio_key)
    stem = p.stem
    parent = str(p.parent).rstrip("/")
    return f"{parent}/{stem}.txt" if parent else f"{stem}.txt"


def transcript_exists(s3, bucket: str, transcript_key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=transcript_key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def build_model(cache_dir: Optional[str] = "cache") -> WhisperModel:
    compute_type = os.getenv("COMPUTE_TYPE", "float16")
    device_index = int(os.getenv("CUDA_DEVICE_INDEX", "0"))
    return WhisperModel(
        "KBLab/kb-whisper-medium",
        device="cuda",
        device_index=device_index,
        compute_type=compute_type,
        download_root="cache",
    )

def make_redis_client():
    print("DEBUG: make_redis_client() called")
    url = os.getenv("REDIS_URL")
    if not url:
        raise ValueError("REDIS_URL environment variable is required")
    # Allow redis-py to parse rediss:// and attach CA
    ca = os.getenv("REDIS_TLS_CA_FILE")
    kwargs: Dict[str, Any] = {}
    if ca:
        kwargs["ssl_ca_certs"] = ca
    client = redis.from_url(url, **kwargs)
    # simple ping to fail fast
    client.ping()
    return client


def ensure_stream_group(r, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)
    except ResponseError as e:  # BUSYGROUP means it already exists
        msg = str(e)
        if "BUSYGROUP" in msg:
            return
        raise


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _cache_paths(cache_root: Path, key: str) -> Dict[str, Path]:
    # keep S3-like layout under cache
    key_norm = key.replace("\\", "/")
    audio_path = cache_root / "audio" / key_norm
    model_root = cache_root / "model"
    out_path = cache_root / "out" / (Path(key_norm).parent / (Path(key_norm).stem + ".txt"))
    return {"audio": audio_path, "model_root": model_root, "out": out_path}


def _download_if_needed(s3, bucket: str, key: str, dest_path: Path) -> None:
    if dest_path.exists() and dest_path.stat().st_size > 0:
        return
    _safe_mkdir(dest_path.parent)
    tmp = dest_path.with_suffix(dest_path.suffix + ".part")
    s3.download_file(bucket, key, str(tmp))
    tmp.replace(dest_path)


def _extract_key_from_message(fields: Dict[bytes, bytes]) -> str:
    # Fields are bytes->bytes; expect b"key": b"podcast/episode/episode.mp3"
    if b"key" in fields:
        return fields[b"key"].decode("utf-8")
    # If a single field contains JSON, allow it as fallback
    for v in fields.values():
        try:
            obj = json.loads(v.decode("utf-8"))
            if isinstance(obj, dict) and "key" in obj:
                return str(obj["key"])
        except Exception:
            continue
    raise ValueError("Message missing 'key' field")


def process_message(r, s3, bucket: str, model: WhisperModel, cache_root: Path, message: Any, consumer: str, lock_ttl_sec: int) -> bool:
    stream, msg_id, fields = message
    key = _extract_key_from_message(fields)

    # Idempotent skip if transcript exists already
    t_key = transcript_key_for(key)
    if transcript_exists(s3, bucket, t_key):
        return True

    # Per-key lock to avoid duplicate concurrent processing across duplicate messages
    lock_key = f"lock:transcribe:{t_key}"
    try:
        got_lock = r.set(lock_key, consumer, nx=True, ex=lock_ttl_sec)
    except Exception as e:
        print(f"Redis SET failed for lock {lock_key}: {e}")
        traceback.print_exc()
        raise
    if not got_lock:
        # Another worker is actively processing this transcript; do not ack this duplicate
        return False

    try:
        paths = _cache_paths(cache_root, key)
        _safe_mkdir(paths["audio"].parent)
        _safe_mkdir(paths["out"].parent)

        _download_if_needed(s3, bucket, key, paths["audio"])

        result = transcribe_file(model, paths["audio"])  # returns segments
        plain_text = "\n".join(seg["text"].strip() for seg in result["segments"])

        paths["out"].write_text(plain_text, encoding="utf-8")
        if not transcript_exists(s3, bucket, t_key):
            s3.upload_file(str(paths["out"]), bucket, t_key)

        return True
    finally:
        try:
            r.delete(lock_key)
        except Exception as e:
            print(f"Failed to delete lock {lock_key}: {e}")
            traceback.print_exc()


def redis_worker_loop() -> None:
    print("WORKER: Entering redis_worker_loop")
    # Setup clients and cache/model
    r = make_redis_client()
    print("WORKER: Redis client created")
    s3, bucket = make_s3_client()
    print("WORKER: S3 client created")
    # Cache root keeps downloaded audio/model weights persistent across batches
    cache_root = Path(os.getenv("CACHE_DIR", "/cache")).resolve()
    _safe_mkdir(cache_root)

    # Ensure the Redis consumer group exists so we can xreadgroup without errors
    print("WORKER: Ensuring stream group")
    ensure_stream_group(r, "podcast:queue", "workers")
    print("WORKER: Stream group ready")

    # Build model once per pod, cache weights under cache_root/model
    print("WORKER: Building model (this may take a while)...")
    model = build_model(cache_dir=str(cache_root / "model"))
    print("WORKER: Model loaded")

    consumer = f"{socket.gethostname()}-{os.getpid()}"
    stream = "podcast:queue"
    group = "workers"

    # Batch size controls how many jobs we pull per GPU inference cycle
    gpu_batch_size = int(os.getenv("GPU_BATCH_SIZE", "16"))
    # DOWNLOAD_WORKERS sets how many threads we devote to prefetching audio
    download_workers = int(os.getenv("DOWNLOAD_WORKERS", "4"))
    prefetch_multiplier = int(os.getenv("PREFETCH_MULTIPLIER", "2"))
    prefetch_count = max(gpu_batch_size, gpu_batch_size * prefetch_multiplier)

    lock_ttl_sec = int(os.getenv("TRANSCRIBE_LOCK_TTL_SEC", str(int(timedelta(hours=6).total_seconds()))))

    while True: #? We could probably make this a bit better, i can see some drop in utalization time of the GPU.
        try:
            # Read multiple messages at once for batch processing (prefetch window keeps GPU fed)
            msgs = r.xreadgroup(groupname=group, consumername=consumer, streams={stream: ">"}, count=prefetch_count, block=5000)
            if not msgs:
                continue
            
            # Collect messages for batch processing
            batch_messages = []
            for _stream, items in msgs:
                for msg_id, fields in items:
                    batch_messages.append((_stream, msg_id, fields))
            print(f"Redis worker fetched {len(batch_messages)} message(s) from stream")
            
            # Process batch if we have multiple messages
            if len(batch_messages) > 1:
                # Prepare batch metadata (download lazily via queue)
                valid_messages: List[Dict[str, Any]] = []
                
                for index, (stream_name, msg_id, fields) in enumerate(batch_messages):
                    try:
                        key = _extract_key_from_message(fields)
                        t_key = transcript_key_for(key)
                        
                        # Try to get lock
                        lock_key = f"lock:transcribe:{t_key}"
                        got_lock = r.set(lock_key, consumer, nx=True, ex=lock_ttl_sec)
                        if not got_lock:
                            # Another worker already grabbed this key
                            print(f"Skipping {key}: lock already held by another consumer")
                            continue  # Skip this message
                        
                        paths = _cache_paths(cache_root, key)
                        _safe_mkdir(paths["out"].parent)
                        print(f"Queued {key} for batch download (index {index})")

                        valid_messages.append(
                            {
                                "index": index,
                                "stream_name": stream_name,
                                "msg_id": msg_id,
                                "fields": fields,
                                "t_key": t_key,
                                "lock_key": lock_key,
                                "paths": paths,
                                "key": key,
                            }
                        )
                    except Exception as e:
                        print(f"Prep error for {msg_id}: {e}")
                
                # Batch transcribe if we have files
                if valid_messages:
                    try:
                        download_queue: queue.Queue = queue.Queue(maxsize=max(1, gpu_batch_size * 2))
                        download_complete = threading.Event()

                        def download_worker(entry: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
                            try:
                                print(f"Downloading {entry['key']} to cache")
                                _download_if_needed(s3, bucket, entry["key"], entry["paths"]["audio"])
                                print(f"Finished download for {entry['key']}")
                            except Exception as err:
                                # Flag the entry so we can release its lock later
                                entry["download_error"] = err
                            return entry["index"], entry

                        def download_producer() -> None:
                            # Spin up a thread pool to fetch audio concurrently
                            with ThreadPoolExecutor(max_workers=download_workers) as executor:
                                futures = [executor.submit(download_worker, entry) for entry in valid_messages]
                                for future in futures:
                                    try:
                                        item_index, entry = future.result()
                                        download_queue.put((item_index, entry))
                                    except Exception as err:
                                        print(f"Download future error: {err}")
                            download_complete.set()

                        threading.Thread(target=download_producer, daemon=True).start()
                        print(f"Download producer thread started (prefetch_count={prefetch_count})")

                        downloaded_entries: List[Optional[Dict[str, Any]]] = [None] * len(valid_messages)
                        ready_entries: List[Dict[str, Any]] = []

                        def record_entry(item_index: int, entry: Dict[str, Any]) -> None:
                            downloaded_entries[item_index] = entry
                            if entry and "download_error" not in entry:
                                ready_entries.append(entry)

                        # Prime the ready buffer with completed downloads
                        while len(ready_entries) < gpu_batch_size and not download_complete.is_set():
                            try:
                                item_index, entry = download_queue.get(timeout=0.5)
                                record_entry(item_index, entry)
                            except queue.Empty:
                                pass

                        # Drain any additional finished downloads without blocking
                        while not download_queue.empty():
                            try:
                                item_index, entry = download_queue.get_nowait()
                                record_entry(item_index, entry)
                            except queue.Empty:
                                break

                        if not ready_entries and download_complete.is_set():
                            print("No entries ready after download stage; retrying next loop")
                            for entry in downloaded_entries:
                                if entry and "download_error" in entry:
                                    print(f"Download error for {entry['key']}: {entry['download_error']}")
                                    try:
                                        r.delete(entry["lock_key"])
                                    except Exception:
                                        pass
                            continue

                        batch_num = 0
                        while ready_entries or not download_complete.is_set():
                            if not ready_entries:
                                try:
                                    item_index, entry = download_queue.get(timeout=0.5)
                                    record_entry(item_index, entry)
                                    continue
                                except queue.Empty:
                                    if download_complete.is_set():
                                        break
                                    continue

                            batch = ready_entries[:gpu_batch_size]
                            ready_entries = ready_entries[len(batch):]

                            batch_num += 1
                            batch_paths = [entry["paths"]["audio"] for entry in batch]
                            print(f"Submitting batch #{batch_num} of {len(batch_paths)} file(s) to transcribe (overlapping with remaining downloads)")

                            results = transcribe_batch(model, batch_paths, batch_size=gpu_batch_size)

                            # Process results
                            for entry, result in zip(batch, results):
                                try:
                                    if "error" not in result:
                                        plain_text = "\n".join(seg["text"].strip() for seg in result["segments"])
                                        entry["paths"]["out"].write_text(plain_text, encoding="utf-8")

                                        if not transcript_exists(s3, bucket, entry["t_key"]):
                                            s3.upload_file(str(entry["paths"]["out"]), bucket, entry["t_key"])

                                        r.xack(stream, group, entry["msg_id"])
                                        r.incr("podcast:processed_count")
                                        print(f"Transcribed and uploaded transcript for {entry['key']}")
                                    else:
                                        print(f"Batch result for {entry['t_key']} failed: {result.get('error')}")
                                finally:
                                    try:
                                        r.delete(entry["lock_key"])
                                    except Exception:
                                        pass

                            # Collect downloads that completed while the GPU was busy
                            while not download_queue.empty():
                                try:
                                    item_index, entry = download_queue.get_nowait()
                                    record_entry(item_index, entry)
                                except queue.Empty:
                                    break

                        # Release locks for entries that failed download
                        for entry in downloaded_entries:
                            if not entry:
                                continue
                            if "download_error" in entry:
                                print(f"Download error for {entry['key']}: {entry['download_error']}")
                                try:
                                    r.delete(entry["lock_key"])
                                except Exception:
                                    pass

                        print(f"Completed {batch_num} GPU batch(es) from prefetch window")
                    except Exception as e:
                        print(f"Batch processing error: {e}")
                        traceback.print_exc()
            else:
                # Single message, process normally
                for _stream, items in msgs:
                    for msg_id, fields in items:
                        try:
                            ok = process_message(r, s3, bucket, model, cache_root, (_stream, msg_id, fields), consumer, lock_ttl_sec)
                            if ok:
                                r.xack(stream, group, msg_id)
                                r.incr("podcast:processed_count")
                                print(f"Processed single message {msg_id}")
                        except ClientError as e:
                            print(f"S3 error for {msg_id}: {e}")
                            traceback.print_exc()
                        except Exception as e:
                            print(f"Worker error for {msg_id}: {e}")
                            traceback.print_exc()
        except KeyboardInterrupt:
            raise
        except Exception as loop_err:
            # backoff minimal to avoid hot loop
            print(f"Loop error: {loop_err}")
            time.sleep(1.0)


def transcribe_file(model: WhisperModel, audio_path: Path) -> Dict[str, Any]:
    segments, info = model.transcribe(
        str(audio_path),
        language="sv",
        task="transcribe",
        vad_filter=True,
        beam_size=0,
        temperature=0.0,
        condition_on_previous_text=False,
    )
    collected = {
        "language": getattr(info, "language", None),
        "language_probability": getattr(info, "language_probability", None),
        "segments": [],
    }
    for seg in segments:
        collected["segments"].append({
            "id": seg.id,
            "start": seg.start,
            "end": seg.end,
            "text": seg.text,
        })
    return collected


def transcribe_batch(model: WhisperModel, audio_paths: List[Path], batch_size: int = 8) -> List[Dict[str, Any]]:
    """Process multiple audio files in parallel batches on GPU.
    
    With 8xH200 GPUs, we can process multiple files simultaneously.
    Returns results in same order as input paths.
    """
    results = []
    
    # Process in batches
    for i in range(0, len(audio_paths), batch_size):
        batch_paths = audio_paths[i:i + batch_size]
        batch_results = []
        
        # Use ThreadPoolExecutor for parallel GPU inference
        # Each thread will use the same model but process different audio
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = []
            for path in batch_paths:
                futures.append(executor.submit(transcribe_file, model, path))
        
            for future in futures:
                try:
                    result = future.result()  # 10 min timeout per file
                    batch_results.append(result)
                except Exception as e:
                    print(f"Batch transcription error ({type(e).__name__}): {e}")
                    traceback.print_exc()
                    batch_results.append({"segments": [], "error": f"{type(e).__name__}: {e}"})
        
        results.extend(batch_results)
    
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transcribe podcasts via Redis worker (default) or helper modes")
    parser.add_argument("--enqueue-missing", action="store_true", help="Producer: enqueue S3 audio keys missing transcripts")
    parser.add_argument("--redis-stream", type=str, default="podcast:queue", help="Redis stream name (default: podcast:queue)")
    parser.add_argument("--redis-worker", action="store_true", help="Run as Redis stream consumer worker")
    return parser.parse_args()


def main() -> None:
    print("main function started")
    args = parse_args()

    if args.redis_worker:
        redis_worker_loop()
        return
    # ! This is the producer mode we use to enqueue transcripts to 
    if args.enqueue_missing: # this is the mode we use to enqueue missing transcripts
        # Producer mode: scan S3, dedupe, enqueue only missing transcripts
        s3, bucket = make_s3_client()
        r = make_redis_client()
        stream_name = args.redis_stream
        s3_prefix = os.getenv("S3_PREFIX")

        # Ensure stream exists; fail visibly if it cannot be created
        try:
            ensure_stream_group(r, stream_name, "workers")
        except Exception as e:
            print(f"ensure_stream_group failed for {stream_name}: {e}")
            traceback.print_exc()
            raise

        keys = list_audio_keys(s3, bucket, s3_prefix)
        total = 0
        enq = 0
        for key in keys:
            total += 1
            # Redis-side de-dup window; prevents enqueueing the same key repeatedly
            dedup_key = f"queue:dedup:{key}"
            try:
                dedup_ok = r.set(dedup_key, "1", nx=True)
            except Exception as e:
                print(f"Producer dedup SET failed for {dedup_key}: {e}")
                traceback.print_exc()
                raise
            if dedup_ok:
                r.xadd(stream_name, {"key": key})
                enq += 1
        print(f"Scanned {total} keys, enqueued {enq} missing transcripts to {stream_name}")
        return


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


