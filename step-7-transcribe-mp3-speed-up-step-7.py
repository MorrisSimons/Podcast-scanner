import os
import sys
import argparse
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from faster_whisper import WhisperModel
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm
import json
import time
import socket
from datetime import timedelta
import traceback

try:
    import redis  # type: ignore
    from redis.exceptions import ResponseError  # type: ignore
except Exception as _e:
    redis = None  # lazily validated when --redis-worker is used
    ResponseError = Exception  # fallback typing

load_dotenv()


def make_s3_client():
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
    return s3, bucket


def list_audio_keys(s3, bucket: str, prefix: Optional[str]) -> List[str]:
    """List audio object keys in S3 that do not yet have a transcript.

    Why: We avoid wasting bandwidth/GPU time by skipping files that already
    have a transcript uploaded next to them in S3.
    """
    # Only consider files with known audio extensions
    #TODO: i dont know have audio files extensions is in the data but my guess is that is only mp3
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


def download_from_s3(s3, bucket: str, key: str) -> Path:
    tmp_dir = Path(tempfile.mkdtemp(prefix="transcribe_"))
    local_path = tmp_dir / Path(key).name
    s3.download_file(bucket, key, str(local_path))
    return local_path


def build_model(cache_dir: Optional[str] = "cache") -> WhisperModel:
    device = "cuda"  # node is GPU; fail fast if CUDA unavailable
    compute_type = os.getenv("COMPUTE_TYPE", "float16")
    device_index = int(os.getenv("CUDA_DEVICE_INDEX", "0"))
    return WhisperModel(
        "KBLab/kb-whisper-large",
        device=device,
        device_index=device_index,
        compute_type=compute_type,
        download_root=cache_dir,
    )


def _get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def make_redis_client():
    if redis is None:
        raise RuntimeError("redis-py is not installed; install 'redis' package to use --redis-worker")
    url = _get_env("REDIS_URL")
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
    # Setup clients and cache/model
    r = make_redis_client()
    s3, bucket = make_s3_client()
    cache_root = Path(os.getenv("CACHE_DIR", "/cache")).resolve()
    _safe_mkdir(cache_root)

    ensure_stream_group(r, "podcast:queue", "workers")

    # Build model once per pod, cache weights under cache_root/model
    model = build_model(cache_dir=str(cache_root / "model"))

    consumer = f"{socket.gethostname()}-{os.getpid()}"
    stream = "podcast:queue"
    group = "workers"

    gpu_batch_size = int(os.getenv("GPU_BATCH_SIZE", "64"))

    lock_ttl_sec = int(os.getenv("TRANSCRIBE_LOCK_TTL_SEC", str(int(timedelta(hours=6).total_seconds()))))

    while True:
        try:
            # Read multiple messages at once for batch processing
            msgs = r.xreadgroup(groupname=group, consumername=consumer, streams={stream: ">"}, count=gpu_batch_size, block=5000)
            if not msgs:
                continue
            
            # Collect messages for batch processing
            batch_messages = []
            for _stream, items in msgs:
                for msg_id, fields in items:
                    batch_messages.append((_stream, msg_id, fields))
            
            # Process batch if we have multiple messages
            if len(batch_messages) > 1:
                # Prepare batch
                batch_keys = []
                batch_paths = []
                valid_messages = []
                
                for stream_name, msg_id, fields in batch_messages:
                    try:
                        key = _extract_key_from_message(fields)
                        t_key = transcript_key_for(key)
                        
                        # Skip if transcript exists
                        if transcript_exists(s3, bucket, t_key):
                            r.xack(stream, group, msg_id)
                            continue
                        
                        # Try to get lock
                        lock_key = f"lock:transcribe:{t_key}"
                        got_lock = r.set(lock_key, consumer, nx=True, ex=lock_ttl_sec)
                        if not got_lock:
                            continue  # Skip this message
                        
                        # Download audio
                        paths = _cache_paths(cache_root, key)
                        _download_if_needed(s3, bucket, key, paths["audio"])
                        
                        batch_keys.append(key)
                        batch_paths.append(paths["audio"])
                        valid_messages.append((stream_name, msg_id, fields, t_key, lock_key, paths))
                    except Exception as e:
                        print(f"Prep error for {msg_id}: {e}")
                
                # Batch transcribe if we have files
                if batch_paths:
                    try:
                        results = transcribe_batch(model, batch_paths, batch_size=gpu_batch_size)

                        # Process results
                        for (stream_name, msg_id, fields, t_key, lock_key, paths), result in zip(valid_messages, results):
                            try:
                                if "error" not in result:
                                    plain_text = "\n".join(seg["text"].strip() for seg in result["segments"])
                                    paths["out"].write_text(plain_text, encoding="utf-8")

                                    if not transcript_exists(s3, bucket, t_key):
                                        s3.upload_file(str(paths["out"]), bucket, t_key)

                                    r.xack(stream, group, msg_id)
                                    r.incr("podcast:processed_count")
                                else:
                                    print(f"Batch result for {t_key} failed: {result.get('error')}")
                            finally:
                                try:
                                    r.delete(lock_key)
                                except Exception:
                                    pass
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
        beam_size=2,
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
                    result = future.result(timeout=500)  # 5 min timeout per file
                    batch_results.append(result)
                except Exception as e:
                    print(f"Batch transcription error ({type(e).__name__}): {e}")
                    traceback.print_exc()
                    batch_results.append({"segments": [], "error": f"{type(e).__name__}: {e}"})
        
        results.extend(batch_results)
    
    return results


def iter_local_audio(root: Path) -> Iterable[Path]:
    exts = {".mp3", ".wav", ".m4a", ".ogg", ".flac", ".webm", ".opus"}
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts and not p.name.endswith(".inprogress"):
            yield p


def claim_file(path: Path) -> Optional[Path]:
    try:
        claimed = path.with_name(path.name + ".inprogress")
        path.rename(claimed)  # atomic on same filesystem
        return claimed
    except FileNotFoundError:
        return None
    except OSError:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transcribe podcasts with optional local staging")
    parser.add_argument("--download-only", action="store_true", help="Only download audio from S3 to staging dir")
    parser.add_argument("--staging-dir", type=str, help="Directory for staging (download or local input)")
    parser.add_argument("--max-files", type=int, default=0, help="Limit number of files to process (0 = unlimited)")
    parser.add_argument("--redis-worker", action="store_true", help="Run Redis Streams worker and process queue items")
    parser.add_argument("--enqueue-missing", action="store_true", help="Producer: enqueue S3 audio keys missing transcripts")
    parser.add_argument("--redis-stream", type=str, default="podcast:queue", help="Redis stream name (default: podcast:queue)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.redis_worker:
        redis_worker_loop()
        return

    if args.enqueue_missing:
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
                dedup_ok = r.set(dedup_key, "1", nx=True, ex=24 * 3600)
            except Exception as e:
                print(f"Producer dedup SET failed for {dedup_key}: {e}")
                traceback.print_exc()
                raise
            if dedup_ok:
                r.xadd(stream_name, {"key": key})
                enq += 1
        print(f"Scanned {total} keys, enqueued {enq} missing transcripts to {stream_name}")
        return

    # Determine staging root (download target or local input dir)
    staging_root: Optional[Path] = None
    if args.staging_dir:
        staging_root = Path(args.staging_dir).resolve()
    elif os.getenv("LOCAL_INPUT_DIR"):
        staging_root = Path(os.getenv("LOCAL_INPUT_DIR")).resolve()

    if args.download_only:
        if not staging_root:
            raise ValueError("--staging-dir or LOCAL_INPUT_DIR is required for --download-only")
        staging_root.mkdir(parents=True, exist_ok=True)

        s3, bucket = make_s3_client()
        s3_prefix = os.getenv("S3_PREFIX")
        all_keys = list_audio_keys(s3, bucket, s3_prefix)
        keys = [k for k in all_keys if not transcript_exists(s3, bucket, transcript_key_for(k))]
        if not keys:
            print("Nothing to download (all transcripts exist).")
            return
        count = 0
        for key in tqdm(keys, desc="Downloading", unit="file", ncols=80):
            dest = staging_root / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.exists():
                continue
            s3.download_file(bucket, key, str(dest))
            count += 1
            if args.max_files and count >= args.max_files:
                break
        print(f"Downloaded {count} files to {staging_root}")
        return

    # Local-input fast path with batch processing
    if staging_root and staging_root.exists():
        model = build_model()
        s3, bucket = make_s3_client()
        output_dir = Path("output_speach_to_text")
        output_dir.mkdir(exist_ok=True)

        audio_paths = list(iter_local_audio(staging_root))
        if args.max_files:
            audio_paths = audio_paths[:args.max_files]
        
        gpu_batch_size = int(os.getenv("GPU_BATCH_SIZE", "64"))
        processed = 0
        
        # Process in batches
        batch_buffer = []
        for p in tqdm(audio_paths, desc="Transcribing(local)", unit="file", ncols=80):
            claimed = claim_file(p)
            if not claimed:
                continue  # taken by another worker

            rel_key_with_suffix = str(claimed.relative_to(staging_root)).replace(os.sep, "/")
            if rel_key_with_suffix.endswith(".inprogress"):
                rel_key = rel_key_with_suffix[: -len(".inprogress")]
            else:
                rel_key = rel_key_with_suffix

            batch_buffer.append((claimed, rel_key))
            
            # Process batch when full or at end
            if len(batch_buffer) >= gpu_batch_size or p == audio_paths[-1]:
                batch_paths = [item[0] for item in batch_buffer]
                
                try:
                    # Batch transcribe
                    results = transcribe_batch(model, batch_paths, batch_size=gpu_batch_size)
                    
                    # Process results
                    for (claimed_path, rel_key), result in zip(batch_buffer, results):
                        if "error" not in result:
                            plain_text = "\n".join(seg["text"].strip() for seg in result["segments"])

                            # Save locally
                            out_filename = Path(rel_key).stem + ".txt"
                            out_path = output_dir / out_filename
                            out_path.parent.mkdir(parents=True, exist_ok=True)
                            out_path.write_text(plain_text, encoding="utf-8")

                            # Upload to S3
                            t_key = transcript_key_for(rel_key)
                            if not transcript_exists(s3, bucket, t_key):
                                s3.upload_file(str(out_path), bucket, t_key)
                            processed += 1

                        try:
                            claimed_path.unlink()
                        except FileNotFoundError:
                            pass
                except Exception as e:
                    print(f"Batch error: {e}")
                    traceback.print_exc()
                
                # Clear batch buffer
                batch_buffer = []
        
        print(f"\nCompleted transcription for {processed} local file(s).")
        return

    # S3-driven flow with batch processing
    import queue
    import threading
    
    s3, bucket = make_s3_client()
    s3_prefix = os.getenv("S3_PREFIX")
    audio_keys = list_audio_keys(s3, bucket, s3_prefix)
    if not audio_keys:
        print("No audio files found in S3 to transcribe.")
        return

    audio_keys = [k for k in audio_keys if not transcript_exists(s3, bucket, transcript_key_for(k))]
    if not audio_keys:
        print("No audio files require transcription (transcripts already exist).")
        return

    if args.max_files:
        audio_keys = audio_keys[: args.max_files]

    print(f"Found {len(audio_keys)} audio file(s) to consider.")
    model = build_model()

    output_dir = Path("output_speach_to_text")
    output_dir.mkdir(exist_ok=True)

    # Batch size for GPU processing - with 8xH200, we can go aggressive
    gpu_batch_size = int(os.getenv("GPU_BATCH_SIZE", "64"))  

    # Queue to hold downloaded files ready for transcription
    download_queue = queue.Queue(maxsize=gpu_batch_size * 2)  # Buffer 2x batch size
    download_complete = threading.Event()
    processed_count = {"value": 0}
    lock = threading.Lock()
    
    def download_producer():
        """Continuously download files and add to queue"""
        with ThreadPoolExecutor(max_workers=4) as executor:  # More parallel downloads
            futures = []
            
            def download_single(key):
                try:
                    s3_worker, _ = make_s3_client()
                    local_path = download_from_s3(s3_worker, bucket, key)
                    return key, local_path
                except Exception as e:
                    print(f"Download error for {key}: {e}")
                    return key, None
            
            # Submit all downloads
            for key in audio_keys:
                futures.append(executor.submit(download_single, key))
            
            # As downloads complete, add to queue
            for future in as_completed(futures):
                try:
                    key, local_path = future.result()
                    if local_path:
                        download_queue.put((key, local_path))
                except Exception as e:
                    print(f"Error getting download result: {e}")
        
        download_complete.set()
    
    # Start download thread
    download_thread = threading.Thread(target=download_producer, daemon=True)
    download_thread.start()
    
    # Main thread does batch transcription
    with tqdm(total=len(audio_keys), desc="Transcribing", unit="file", ncols=80) as pbar:
        batch_buffer = []

        while True:
            # Collect files for batch processing
            while len(batch_buffer) < gpu_batch_size:
                try:
                    # Short timeout to quickly build batches
                    key, local_path = download_queue.get(timeout=0.5)
                    batch_buffer.append((key, local_path))
                except queue.Empty:
                    if download_complete.is_set():
                        break  # Process remaining batch
                    continue

            # Process batch if we have files
            if batch_buffer:
                batch_keys = [item[0] for item in batch_buffer]
                batch_paths = [item[1] for item in batch_buffer]

                try:
                    # Batch transcribe on GPU
                    results = transcribe_batch(model, batch_paths, batch_size=gpu_batch_size)

                    # Process results and upload
                    for (key, local_path), result in zip(batch_buffer, results):
                        if "error" not in result:
                            plain_text = "\n".join(seg["text"].strip() for seg in result["segments"])
                            output_filename = Path(key).stem + ".txt"
                            output_path = output_dir / output_filename
                            output_path.write_text(plain_text, encoding="utf-8")

                            # Upload transcript
                            transcript_key = transcript_key_for(key)
                            if not transcript_exists(s3, bucket, transcript_key):
                                s3.upload_file(str(output_path), bucket, transcript_key)

                            with lock:
                                processed_count["value"] += 1
                            pbar.update(1)

                        # Clean up temp file
                        try:
                            local_path.unlink()
                            local_path.parent.rmdir()  # Remove temp dir if empty
                        except Exception:
                            pass

                except Exception as e:
                    print(f"Batch processing error: {e}")
                    traceback.print_exc()

                # Clear batch buffer
                batch_buffer = []

            # Check if we're done
            if download_complete.is_set() and not batch_buffer:
                break


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


