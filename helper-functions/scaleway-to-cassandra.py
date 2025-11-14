#!/usr/bin/env python3

"""
Download all .txt files from Scaleway S3 and store them in Cassandra database.
"""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional, Tuple, Any

import boto3
from botocore.exceptions import ClientError
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()


def make_s3_client():
    """Create S3 client for Scaleway using environment variables."""
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    bucket = os.getenv("S3_BUCKET")
    
    if not endpoint_url or not bucket:
        raise ValueError("S3_ENDPOINT_URL and S3_BUCKET are required")
    
    # Fix endpoint if it contains bucket name (Scaleway specific)
    if bucket and f"{bucket}." in endpoint_url:
        endpoint_url = endpoint_url.replace(f"{bucket}.", "")
    
    config = boto3.session.Config(s3={'addressing_style': 'path'})
    
    s3 = boto3.session.Session().client(
        service_name="s3",
        region_name=os.getenv("S3_REGION"),
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
        config=config,
    )
    
    return s3, bucket


def connect_cassandra(
    host: str,
    username: str,
    password: str,
    keyspace: str
) -> Tuple[Cluster, Any]:
    """Connect to Cassandra cluster and return session."""
    auth = PlainTextAuthProvider(username, password)
    cluster = Cluster([host], auth_provider=auth)
    session = cluster.connect(keyspace)
    return cluster, session


def create_table_if_not_exists(session, keyspace: str) -> None:
    """Create transcript_files table if it doesn't exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.transcript_files (
        filename text PRIMARY KEY,
        content text,
        file_size bigint,
        s3_key text,
        downloaded_at timestamp
    )
    """
    session.execute(SimpleStatement(create_table_query))
    print(f"Table 'transcript_files' ready in keyspace '{keyspace}'")


def list_txt_files(s3, bucket: str, prefix: Optional[str] = None, limit: Optional[int] = None) -> list[dict]:
    """List .txt files in S3 bucket, optionally stopping after finding limit files."""
    paginator = s3.get_paginator("list_objects_v2")
    params = {"Bucket": bucket}
    if prefix:
        params["Prefix"] = prefix
    
    txt_files = []
    if limit:
        print(f"Scanning S3 for first {limit} .txt files...")
    else:
        print("Scanning S3 for .txt files...")
    
    try:
        for page in paginator.paginate(**params):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if key and key.lower().endswith(".txt"):
                    txt_files.append({
                        "key": key,
                        "size": obj.get("Size", 0),
                        "last_modified": obj.get("LastModified")
                    })
                    # Stop early if we have enough files
                    if limit and len(txt_files) >= limit:
                        return txt_files
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code not in ("NoSuchKey", "404", "NotFound"):
            raise
    
    return txt_files


def download_file(s3, bucket: str, s3_key: str, encoding: str = "utf-8") -> Optional[dict]:
    """Download a file from S3 and return its data."""
    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        content = response["Body"].read().decode(encoding)
        filename = s3_key.split("/")[-1]
        file_size = len(content.encode(encoding))
        
        return {
            "filename": filename,
            "content": content,
            "file_size": file_size,
            "s3_key": s3_key,
            "downloaded_at": datetime.now()
        }
    except Exception as e:
        print(f"ERROR downloading {s3_key}: {e}")
        return None


def create_s3_client():
    """Create a new S3 client (thread-safe, each thread gets its own)."""
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    bucket = os.getenv("S3_BUCKET")
    
    if not endpoint_url or not bucket:
        raise ValueError("S3_ENDPOINT_URL and S3_BUCKET are required")
    
    if bucket and f"{bucket}." in endpoint_url:
        endpoint_url = endpoint_url.replace(f"{bucket}.", "")
    
    config = boto3.session.Config(s3={'addressing_style': 'path'})
    
    return boto3.session.Session().client(
        service_name="s3",
        region_name=os.getenv("S3_REGION"),
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
        config=config,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download .txt files from Scaleway S3 and store them in Cassandra."
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of files to process (useful for testing).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=20,
        help="Number of parallel workers (default: 20).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of files to batch together for Cassandra inserts (default: 50).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    # S3 configuration
    s3, bucket = make_s3_client()
    s3_prefix = os.getenv("S3_PREFIX", "")
    if s3_prefix == "":
        s3_prefix = None
    
    # Cassandra configuration
    cassandra_host = os.getenv("CASSANDRA_HOST")
    cassandra_username = os.getenv("CASSANDRA_USERNAME")
    cassandra_password = os.getenv("CASSANDRA_PASSWORD")
    cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE")
    
    missing = [
        var for var, value in [
            ("CASSANDRA_HOST", cassandra_host),
            ("CASSANDRA_USERNAME", cassandra_username),
            ("CASSANDRA_PASSWORD", cassandra_password),
            ("CASSANDRA_KEYSPACE", cassandra_keyspace),
        ] if not value
    ]
    if missing:
        raise ValueError(f"Missing required Cassandra environment variables: {', '.join(missing)}")
    
    # Connect to Cassandra
    print(f"Connecting to Cassandra at {cassandra_host}...")
    cluster, session = connect_cassandra(
        cassandra_host,
        cassandra_username,
        cassandra_password,
        cassandra_keyspace
    )
    
    # Create table if needed
    create_table_if_not_exists(session, cassandra_keyspace)
    
    # Prepare statements
    insert_query = """
    INSERT INTO transcript_files (filename, content, file_size, s3_key, downloaded_at)
    VALUES (%s, %s, %s, %s, %s)
    """
    prepared = session.prepare(insert_query)
    
    check_query = "SELECT filename FROM transcript_files WHERE filename = %s"
    check_prepared = session.prepare(check_query)
    
    # List .txt files (stop early if limit is specified)
    txt_files = list_txt_files(s3, bucket, s3_prefix, limit=args.limit)
    
    if not txt_files:
        print("No .txt files found in S3 bucket")
        return
    
    print(f"Found {len(txt_files)} .txt files in S3")
    
    # Check which files already exist in Cassandra (in parallel)
    print("Checking which files are already in Cassandra...")
    existing_filenames = set()
    
    def check_exists(file_info: dict) -> Optional[str]:
        """Check if file exists, return filename if it does, None otherwise."""
        filename = file_info["key"].split("/")[-1]
        try:
            result = session.execute(check_prepared, (filename,))
            if result.one():
                return filename
        except Exception:
            pass
        return None
    
    # Check in parallel batches
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        results = list(tqdm(
            executor.map(check_exists, txt_files),
            total=len(txt_files),
            desc="Checking existing files",
            unit="file"
        ))
        existing_filenames = {f for f in results if f is not None}
    
    # Filter out files that already exist
    files_to_process = [
        f for f in txt_files 
        if f["key"].split("/")[-1] not in existing_filenames
    ]
    
    skipped_count = len(txt_files) - len(files_to_process)
    if skipped_count > 0:
        print(f"Skipping {skipped_count} files that already exist in Cassandra")
    
    if not files_to_process:
        print("All files are already in Cassandra. Nothing to do.")
        return
    
    print(f"Processing {len(files_to_process)} new files")
    print(f"Using {args.workers} parallel workers with batch size {args.batch_size}")
    
    # Process files in parallel with batching
    success_count = 0
    error_count = 0
    
    def process_batch(file_batch: list[dict]) -> Tuple[int, int]:
        """Process a batch of files: download from S3 and insert into Cassandra."""
        batch_success = 0
        batch_errors = 0
        
        # Create S3 client for this thread
        thread_s3 = create_s3_client()
        
        # Download all files in batch
        file_data_list = []
        for file_info in file_batch:
            data = download_file(thread_s3, bucket, file_info["key"])
            if data:
                file_data_list.append(data)
            else:
                batch_errors += 1
        
        if not file_data_list:
            return batch_success, batch_errors
        
        # Batch insert into Cassandra
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for data in file_data_list:
            batch.add(prepared, (
                data["filename"],
                data["content"],
                data["file_size"],
                data["s3_key"],
                data["downloaded_at"]
            ))
        
        session.execute(batch)
        batch_success = len(file_data_list)
        
        return batch_success, batch_errors
    
    # Split files into batches
    batches = []
    for i in range(0, len(files_to_process), args.batch_size):
        batches.append(files_to_process[i:i + args.batch_size])
    
    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_batch, batch): batch for batch in batches}
        
        for future in tqdm(as_completed(futures), total=len(batches), desc="Processing batches", unit="batch"):
            try:
                batch_success, batch_errors = future.result()
                success_count += batch_success
                error_count += batch_errors
            except Exception as e:
                print(f"ERROR in batch processing: {e}")
                error_count += len(futures[future])
    
    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY:")
    print(f"  Total files found: {len(txt_files)}")
    print(f"  Already in database: {skipped_count}")
    print(f"  Files processed: {len(files_to_process)}")
    print(f"  Success: {success_count}")
    print(f"  Errors: {error_count}")
    print(f"{'='*60}")
    
    cluster.shutdown()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
