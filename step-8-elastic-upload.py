#!/usr/bin/env python3

"""Upload transcript text files to Elasticsearch with unique keyword metadata."""

import argparse
import os
import re
from pathlib import Path
from typing import Optional

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

from elasticsearch import Elasticsearch, helpers

TOKEN_PATTERN = re.compile(r"\b\w+\b")


class SupabaseRestClient:
    def __init__(self, base_url: str, service_role_key: str) -> None:
        self.base_url = base_url.rstrip("/") + "/rest/v1"
        self.api_key = service_role_key
        self.session = requests.Session()
        self.default_headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def get(self, path: str, params: Optional[dict] = None) -> requests.Response:
        url = self.base_url + path
        return self.session.get(url, params=params or {}, headers=self.default_headers)


def make_s3_client():
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    bucket = os.getenv("S3_BUCKET")
    
    if not endpoint_url or not bucket:
        raise ValueError("S3_ENDPOINT_URL and S3_BUCKET are required")
    
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload text transcripts to Elasticsearch with keyword facets.",
    )
    parser.add_argument(
        "--input-dir",
        default="output_speach_to_text",
        help="Directory containing .txt files to index.",
    )
    parser.add_argument(
        "--use-s3",
        action="store_true",
        help="Read transcript files from Scaleway S3 instead of local directory.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of files to process (useful for testing).",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Encoding used when reading transcript files.",
    )
    parser.add_argument(
        "--index",
        default=os.getenv("ELASTICSEARCH_INDEX", "podcast-transcripts"),
        help="Elasticsearch index name to write to.",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("ELASTICSEARCH_ENDPOINT"),
        help="Elasticsearch host URL.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ELASTICSEARCH_APIKEY"),
        help="Elasticsearch API key (defaults to ELASTICSEARCH_APIKEY).",
    )
    parser.add_argument(
        "--delete-index",
        action="store_true",
        help="Delete the target index before indexing new documents.",
    )
    return parser.parse_args()


def collect_documents(directory: Path, encoding: str) -> list[dict[str, object]]:
    txt_files = sorted(directory.glob("*.txt"))
    if not txt_files:
        raise FileNotFoundError(f"No .txt files found in {directory}")

    documents: list[dict[str, object]] = []
    for file_path in txt_files:
        text = file_path.read_text(encoding=encoding)
        unique_keywords = sorted(_unique_tokens(text))
        if not unique_keywords:
            continue
        documents.append(
            {
                "id": str(file_path.relative_to(directory)),
                "filename": file_path.name,
                "path": str(file_path.resolve()),
                "content": text,
                "unique_keywords": unique_keywords,
            }
        )

    if not documents:
        raise ValueError(f"No indexable content found in files under {directory}")

    return documents


def _unique_tokens(text: str) -> set[str]:
    return {match for match in TOKEN_PATTERN.findall(text.lower())}


def collect_documents_from_s3(encoding: str, limit: Optional[int] = None) -> list[dict[str, object]]:
    s3, bucket = make_s3_client()
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    
    supabase_client = SupabaseRestClient(supabase_url, supabase_key)
    
    s3_prefix = os.getenv("S3_PREFIX", "")
    if s3_prefix == "":
        s3_prefix = None
    
    paginator = s3.get_paginator("list_objects_v2")
    params = {"Bucket": bucket}
    if s3_prefix:
        params["Prefix"] = s3_prefix
    
    documents: list[dict[str, object]] = []
    
    print("Listing .txt files from S3...")
    txt_keys = []
    for page in paginator.paginate(**params):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if key and key.lower().endswith(".txt"):
                txt_keys.append(key)
    
    print(f"Found {len(txt_keys)} .txt files in S3")
    
    if limit:
        txt_keys = txt_keys[:limit]
        print(f"Limiting to first {len(txt_keys)} files for testing")
    
    for idx, key in enumerate(txt_keys, 1):
        filename = key.split("/")[-1]
        episode_id = filename.rsplit(".", 1)[0]
        
        print(f"[{idx}/{len(txt_keys)}] Processing episode {episode_id}...")
        
        response = s3.get_object(Bucket=bucket, Key=key)
        text = response["Body"].read().decode(encoding)
        
        unique_keywords = sorted(_unique_tokens(text))
        if not unique_keywords:
            print(f"  Skipping {episode_id} - no keywords found")
            continue
        
        resp = supabase_client.get(
            "/episodes",
            params={
                "id": f"eq.{episode_id}",
                "select": "id,title,description,pub_date,duration_seconds,episode_number,season_number,audio_url,link_url,keywords,podcasts(id,title,author,categories,image_url,language,rss_feed_url)"
            }
        )
        
        if resp.status_code != 200:
            print(f"  Warning: Failed to fetch metadata for {episode_id}: HTTP {resp.status_code}")
            continue
        
        rows = resp.json()
        if not rows:
            print(f"  Warning: No metadata found for {episode_id}")
            continue
        
        episode_data = rows[0]
        podcast_data = episode_data.pop("podcasts", None)
        
        doc = {
            "id": episode_id,
            "content": text,
            "unique_keywords": unique_keywords,
            "episode_id": episode_data.get("id"),
            "episode_title": episode_data.get("title"),
            "episode_description": episode_data.get("description"),
            "episode_pub_date": episode_data.get("pub_date"),
            "episode_duration_seconds": episode_data.get("duration_seconds"),
            "episode_number": episode_data.get("episode_number"),
            "episode_season_number": episode_data.get("season_number"),
            "episode_audio_url": episode_data.get("audio_url"),
            "episode_link_url": episode_data.get("link_url"),
            "episode_keywords": episode_data.get("keywords") or [],
        }
        
        if podcast_data:
            doc.update({
                "podcast_id": podcast_data.get("id"),
                "podcast_title": podcast_data.get("title"),
                "podcast_author": podcast_data.get("author"),
                "podcast_categories": podcast_data.get("categories") or [],
                "podcast_image_url": podcast_data.get("image_url"),
                "podcast_language": podcast_data.get("language"),
                "podcast_rss_feed_url": podcast_data.get("rss_feed_url"),
            })
        
        documents.append(doc)
    
    if not documents:
        raise ValueError("No indexable documents collected from S3")
    
    print(f"Collected {len(documents)} documents with metadata")
    return documents


def connect(host: str, api_key: Optional[str]) -> Elasticsearch:
    client = Elasticsearch(hosts=[host], api_key=api_key) if api_key else Elasticsearch(hosts=[host])
    if not client.ping():
        raise ConnectionError(f"Failed to reach Elasticsearch at {host}")
    return client


def ensure_index(client: Elasticsearch, index_name: str, delete_existing: bool) -> None:
    if delete_existing and client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)

    if client.indices.exists(index=index_name):
        return

    client.indices.create(
        index=index_name,
        mappings={
            "properties": {
                "filename": {"type": "keyword"},
                "path": {"type": "keyword"},
                "content": {"type": "text"},
                "unique_keywords": {"type": "keyword"},
                "episode_id": {"type": "keyword"},
                "episode_title": {"type": "text"},
                "episode_description": {"type": "text"},
                "episode_pub_date": {"type": "date"},
                "episode_duration_seconds": {"type": "integer"},
                "episode_number": {"type": "integer"},
                "episode_season_number": {"type": "integer"},
                "episode_audio_url": {"type": "keyword"},
                "episode_link_url": {"type": "keyword"},
                "episode_keywords": {"type": "keyword"},
                "podcast_id": {"type": "keyword"},
                "podcast_title": {"type": "text"},
                "podcast_author": {"type": "text"},
                "podcast_categories": {"type": "keyword"},
                "podcast_image_url": {"type": "keyword"},
                "podcast_language": {"type": "keyword"},
                "podcast_rss_feed_url": {"type": "keyword"},
            }
        },
    )


def bulk_index(
    client: Elasticsearch, index_name: str, documents: list[dict[str, object]]
) -> None:
    actions = (
        {
            "_index": index_name,
            "_id": doc["id"],
            "_source": {k: v for k, v in doc.items() if k != "id"},
        }
        for doc in documents
    )
    helpers.bulk(client, actions)


def main() -> None:
    args = parse_args()

    if not args.host:
        raise ValueError(
            "Elasticsearch host is required. Set ELASTICSEARCH_ENDPOINT or pass --host."
        )

    if args.use_s3:
        documents = collect_documents_from_s3(args.encoding, args.limit)
    else:
        input_dir = Path(args.input_dir).expanduser().resolve()
        if not input_dir.exists() or not input_dir.is_dir():
            raise NotADirectoryError(f"Input directory does not exist: {input_dir}")
        documents = collect_documents(input_dir, args.encoding)

    client = connect(args.host, args.api_key)
    ensure_index(client, args.index, args.delete_index)
    bulk_index(client, args.index, documents)
    print(f"Indexed {len(documents)} documents into '{args.index}' at {args.host}")


if __name__ == "__main__":
    main()
