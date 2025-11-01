#!/usr/bin/env python3
"""
SIMPLE S3 MIGRATION SCRIPT - KISS PRINCIPLE
Moves files from {podcast_id}/{episode_id}.mp3 to {podcast_id}/{episode_id}/{episode_id}.mp3
"""
import os
import sys
import boto3
from botocore.exceptions import ClientError
import dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Load environment variables
dotenv.load_dotenv()

# HARDCODED VALUES FOR TEST RUN
TEST_MODE = False  # Set to False to process ALL files
TEST_PODCAST_ID = "0078ec5b-94b8-46ad-9449-ee63af1907bf"
TEST_EPISODE_ID = "125f940f-4d89-46fa-9fe7-7c7620d15290"
BUCKET_NAME = os.getenv("S3_BUCKET")
DRY_RUN = False  # Set to False to actually move files
MAX_WORKERS = 100  # Number of parallel threads (safe for S3, adjust if rate limited)


def build_s3_client():
    """Build S3 client with Scaleway credentials"""
    region = os.getenv("S3_REGION", "fr-par")
    endpoint = os.getenv("S3_ENDPOINT_URL")
    access_key = os.getenv("S3_ACCESS_KEY_ID")
    secret_key = os.getenv("S3_SECRET_ACCESS_KEY")
    
    if not all([endpoint, access_key, secret_key]):
        print("ERROR: Missing S3 credentials in .env")
        sys.exit(1)
    
    # Fix endpoint if it contains bucket name (Scaleway specific)
    if ".s3." in endpoint and "://" in endpoint:
        # Extract just the base endpoint: https://s3.REGION.scw.cloud
        parts = endpoint.split(".")
        for i, part in enumerate(parts):
            if part == "s3":
                # Reconstruct from https://s3.REGION.scw.cloud
                base_parts = parts[i:]  # s3.fr-par.scw.cloud
                endpoint = "https://" + ".".join(base_parts)
                break
    
    print(f"Using endpoint: {endpoint}")
    print(f"Using region: {region}")
    print(f"Using bucket: {BUCKET_NAME}")
    
    return boto3.client(
        's3',
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )


def move_single_file(s3_client, podcast_id, episode_id):
    """Move a single file to new structure"""
    # Check if files have bucket prefix (Scaleway specific issue)
    prefix = ""
    try:
        # Quick check to see if files have bucket name prefix
        test_key = f"{BUCKET_NAME}/{podcast_id}/{episode_id}.mp3"
        s3_client.head_object(Bucket=BUCKET_NAME, Key=test_key)
        prefix = f"{BUCKET_NAME}/"
    except:
        pass
    
    old_key = f"{prefix}{podcast_id}/{episode_id}.mp3"
    new_key = f"{prefix}{podcast_id}/{episode_id}/{episode_id}.mp3"
    
    try:
        # Check if source exists
        try:
            s3_client.head_object(Bucket=BUCKET_NAME, Key=old_key)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
        
        # Check if destination already exists
        try:
            s3_client.head_object(Bucket=BUCKET_NAME, Key=new_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
        
        if DRY_RUN:
            pass
        else:
            # Copy to new location
            copy_source = {'Bucket': BUCKET_NAME, 'Key': old_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=BUCKET_NAME, Key=new_key)
            
            # Verify copy by checking size
            old_obj = s3_client.head_object(Bucket=BUCKET_NAME, Key=old_key)
            new_obj = s3_client.head_object(Bucket=BUCKET_NAME, Key=new_key)
            
            if old_obj['ContentLength'] == new_obj['ContentLength']:
                # Delete old file
                s3_client.delete_object(Bucket=BUCKET_NAME, Key=old_key)
            else:
                print(f"ERROR: Size mismatch for {old_key}")
                return False
        
        return True
        
    except Exception as e:
        print(f"ERROR processing {old_key}: {e}")
        return False


def create_s3_client_for_thread():
    """Create a new S3 client for each thread (thread-safe)"""
    region = os.getenv("S3_REGION", "fr-par")
    endpoint = os.getenv("S3_ENDPOINT_URL")
    access_key = os.getenv("S3_ACCESS_KEY_ID")
    secret_key = os.getenv("S3_SECRET_ACCESS_KEY")
    
    # Fix endpoint if it contains bucket name (Scaleway specific)
    if ".s3." in endpoint and "://" in endpoint:
        parts = endpoint.split(".")
        for i, part in enumerate(parts):
            if part == "s3":
                base_parts = parts[i:]
                endpoint = "https://" + ".".join(base_parts)
                break
    
    return boto3.client(
        's3',
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )


def process_file_wrapper(args):
    """Wrapper function for parallel processing"""
    podcast_id, episode_id = args
    # Create a new S3 client for this thread
    s3_client = create_s3_client_for_thread()
    return move_single_file(s3_client, podcast_id, episode_id)


def process_all_files(s3_client):
    """Process ALL files in bucket that match pattern {podcast_id}/{episode_id}.mp3"""
    print("\nScanning bucket for files to migrate...")
    
    # Collect all files first
    files_to_migrate = []
    already_migrated = 0
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME)
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                
                # Remove bucket prefix if it exists
                if key.startswith(f"{BUCKET_NAME}/"):
                    key_without_bucket = key[len(f"{BUCKET_NAME}/"):]
                else:
                    key_without_bucket = key
                
                parts = key_without_bucket.split('/')
                
                # Check if it matches OLD pattern: {podcast_id}/{episode_id}.mp3
                if len(parts) == 2 and parts[1].endswith('.mp3'):
                    podcast_id = parts[0]
                    episode_id = parts[1][:-4]  # Remove .mp3
                    files_to_migrate.append((podcast_id, episode_id))
                
                # Count files already in NEW pattern: {podcast_id}/{episode_id}/{episode_id}.mp3
                elif len(parts) == 3 and parts[2].endswith('.mp3'):
                    already_migrated += 1
    
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("No files found in bucket")
        else:
            print(f"ERROR listing bucket: {e}")
            return
    
    print(f"Found {len(files_to_migrate)} files to migrate")
    print(f"Already migrated: {already_migrated} files")
    
    if len(files_to_migrate) == 0:
        print("No files need migration - all files are already in the new structure!")
        return
    
    print(f"Using {MAX_WORKERS} parallel workers")
    
    # Process files in parallel with progress bar
    success = 0
    errors = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        futures = {executor.submit(process_file_wrapper, args): args 
                  for args in files_to_migrate}
        
        # Process completed tasks with progress bar
        with tqdm(total=len(files_to_migrate), desc="Migrating files", unit="file") as pbar:
            for future in as_completed(futures):
                try:
                    if future.result():
                        success += 1
                    else:
                        errors += 1
                except Exception as e:
                    errors += 1
                    podcast_id, episode_id = futures[future]
                    print(f"\nERROR in thread for {podcast_id}/{episode_id}: {e}")
                
                pbar.update(1)
    
    print(f"\n{'='*60}")
    print(f"SUMMARY: Processed {len(files_to_migrate)} | Success {success} | Errors {errors}")
    print(f"Dry Run: {DRY_RUN}")
    print(f"{'='*60}")


def list_first_files(s3_client, limit=10):
    """List first few files to see what's in the bucket"""
    print("\nFirst files in bucket:")
    count = 0
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME)
        
        for page in pages:
            if 'Contents' not in page:
                print("  Bucket is empty!")
                return
                
            for obj in page['Contents']:
                print(f"  {obj['Key']}")
                count += 1
                if count >= limit:
                    print(f"  ... (showing first {limit} files)")
                    return
    except Exception as e:
        print(f"  ERROR: {e}")


def main():
    print("="*60)
    print("S3 STRUCTURE MIGRATION SCRIPT")
    print("From: {podcast_id}/{episode_id}.mp3")
    print("To:   {podcast_id}/{episode_id}/{episode_id}.mp3")
    print("="*60)
    
    # Build S3 client
    s3_client = build_s3_client()
    
    # First, let's see what's actually in the bucket
    list_first_files(s3_client, 5)
    
    if TEST_MODE:
        print("\n*** TEST MODE - Processing single hardcoded file ***")
        print(f"Podcast ID: {TEST_PODCAST_ID}")
        print(f"Episode ID: {TEST_EPISODE_ID}")
        move_single_file(s3_client, TEST_PODCAST_ID, TEST_EPISODE_ID)
    else:
        print("\n*** FULL MODE - Processing ALL files in bucket ***")
        if DRY_RUN:
            print("*** DRY RUN - No files will be moved ***")
        else:
            print("*** LIVE RUN - Files WILL be moved ***")
            response = input("Are you sure? Type 'yes' to continue: ")
            if response.lower() != 'yes':
                print("Aborted.")
                return
        
        process_all_files(s3_client)


if __name__ == "__main__":
    main()
