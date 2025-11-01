import os
import sys
import boto3
from dotenv import load_dotenv

load_dotenv()


def make_s3_client():
    # Use Scaleway's proper endpoint format (not virtual-hosted style)
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    bucket = os.getenv("S3_BUCKET")
    
    # If endpoint includes bucket name, strip it and use path-style
    if bucket and f"{bucket}." in endpoint_url:
        # Convert from virtual-hosted to path-style: 
        # https://bucket.s3.region.scw.cloud -> https://s3.region.scw.cloud
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
    
    if not bucket:
        raise ValueError("S3_BUCKET is required")
    
    return s3, bucket


def count_txt_files(s3, bucket: str, prefix: str = None) -> tuple[int, int]:
    from botocore.exceptions import ClientError
    
    paginator = s3.get_paginator("list_objects_v2")
    params = {"Bucket": bucket}
    if prefix:
        params["Prefix"] = prefix
    
    txt_count = 0
    total_objects = 0
    
    try:
        for page in paginator.paginate(**params):
            for obj in page.get("Contents", []):
                total_objects += 1
                key = obj.get("Key")
                if key and key.lower().endswith(".txt"):
                    txt_count += 1
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("NoSuchKey", "404", "NotFound"):
            return 0, 0
        raise
    
    return txt_count, total_objects


def main() -> None:
    s3, bucket = make_s3_client()
    s3_prefix = os.getenv("S3_PREFIX")
    
    # Convert empty string to None
    if s3_prefix == "":
        s3_prefix = None
    
    print(f"Bucket: {bucket}")
    if s3_prefix:
        print(f"Prefix: {s3_prefix}")
    
    print("\nCounting files...")
    txt_count, total_objects = count_txt_files(s3, bucket, s3_prefix)
    
    print(f"\nResults:")
    print(f"  Total objects: {total_objects:,}")
    print(f"  Total .txt files: {txt_count:,}")
    
    if total_objects > 0:
        percentage = (txt_count / total_objects) * 100
        print(f"  Percentage: {percentage:.2f}%")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

