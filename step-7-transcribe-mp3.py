import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from faster_whisper import WhisperModel
from botocore.exceptions import ClientError
from dotenv import load_dotenv

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
    audio_suffixes = (".mp3", ".wav", ".m4a", ".ogg", ".flac", ".webm", ".opus")
    paginator = s3.get_paginator("list_objects_v2")
    keys: List[str] = []
    params = {"Bucket": bucket}
    if prefix:
        params["Prefix"] = prefix
    for page in paginator.paginate(**params):
        for obj in page.get("Contents", []) :
            key = obj.get("Key")
            if key and key.lower().endswith(audio_suffixes):
                keys.append(key)
    return keys


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
    import torch
    device = "cuda" if torch.cuda.is_available() else "cpu"
    compute_type = "float16" if device == "cuda" else "int8"
    return WhisperModel("KBLab/kb-whisper-large", device=device, compute_type=compute_type, download_root=cache_dir)


def transcribe_file(model: WhisperModel, audio_path: Path) -> Dict[str, Any]:
    segments, info = model.transcribe(
        str(audio_path),
        language="sv",
        task="transcribe",
        vad_filter=True,
        beam_size=12,
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

def main() -> None:
    s3, bucket = make_s3_client()
    s3_prefix = os.getenv("S3_PREFIX")
    audio_keys = list_audio_keys(s3, bucket, s3_prefix)
    if not audio_keys:
        print("No audio files found in S3 to transcribe.")
        return

    # Limit to 3 files for testing
    audio_keys = audio_keys[:3]
    
    print(f"Found {len(audio_keys)} audio file(s) to consider.")
    model = build_model()

    # Create output directory
    output_dir = Path("output_speach_to_text")
    output_dir.mkdir(exist_ok=True)

    processed = 0
    for key in audio_keys:
        print(f"[download] s3://{bucket}/{key}")
        local_path = download_from_s3(s3, bucket, key)

        print(f"[transcribe] {local_path.name} …")
        result = transcribe_file(model, local_path)

        # Save transcript locally
        plain_text = "\n".join(seg["text"].strip() for seg in result["segments"])
        output_filename = Path(key).stem + ".txt"
        output_path = output_dir / output_filename
        output_path.write_text(plain_text, encoding="utf-8")
        print(f"[saved] {output_path}")

        # Upload transcript to S3 (Scaleway)
        transcript_key = transcript_key_for(key)
        if not transcript_exists(s3, bucket, transcript_key):
            s3.upload_file(str(output_path), bucket, transcript_key)
            print(f"[uploaded] s3://{bucket}/{transcript_key}")
        else:
            print(f"[skip exists] s3://{bucket}/{transcript_key}")
        processed += 1

    print(f"Completed transcription for {processed} file(s).")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)