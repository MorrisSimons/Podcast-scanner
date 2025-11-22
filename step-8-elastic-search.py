#!/usr/bin/env python3

"""Search transcript keywords stored in Elasticsearch."""

import argparse
import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from elasticsearch import Elasticsearch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search for a keyword within the unique keyword field of an index.",
    )
    parser.add_argument(
        "--index",
        default=os.getenv("ELASTICSEARCH_INDEX"),
        help="Elasticsearch index name to search.",
    )
    parser.add_argument(
        "--host",
        default="http://100.116.226.118:9200",
        help="Elasticsearch host URL. Defaults to local instance: http://100.116.226.118:9200 (or set ELASTICSEARCH_ENDPOINT env var to override).",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ELASTICSEARCH_APIKEY"),
        help="Elasticsearch API key (defaults to ELASTICSEARCH_APIKEY).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of matching documents to return.",
    )
    parser.add_argument(
        "keyword",
        help="Keyword to search for (case insensitive).",
    )
    return parser.parse_args()


def connect(host: str, api_key: Optional[str]) -> Elasticsearch:
    import elasticsearch
    
    # Check if client version matches server version
    client_version = elasticsearch.__version__
    if client_version[0] == 9:
        # Version 9 client requires Elasticsearch 9 server
        # For Elasticsearch 8, we need elasticsearch-py 8.x
        raise ValueError(
            f"Version mismatch: elasticsearch-py {client_version[0]}.x is installed, "
            f"but Elasticsearch 8.x requires elasticsearch-py 8.x. "
            f"Please install compatible version: pip install 'elasticsearch>=8.0.0,<9.0.0'"
        )
    
    # Determine if this is an HTTP (local) or HTTPS (cloud) connection
    is_local = host.startswith("http://")
    
    # Base configuration for all connections
    es_config = {
        "hosts": [host],
    }
    
    if is_local:
        # Local Elasticsearch instance without security
        # SSL is automatically disabled for HTTP URLs
        # Don't verify certificates for local instances
        es_config.update({
            "verify_certs": False,
            "ssl_show_warn": False,
        })
    else:
        # Cloud/managed Elasticsearch instance (HTTPS)
        # SSL is automatically enabled for HTTPS URLs
        es_config.update({
            "verify_certs": True,
        })
        if api_key:
            es_config["api_key"] = api_key
    
    client = Elasticsearch(**es_config)
    
    try:
        if not client.ping():
            raise ConnectionError(f"Failed to reach Elasticsearch at {host}. Is Elasticsearch running?")
    except Exception as e:
        error_msg = str(e)
        # Check for version mismatch error
        if "compatible-with=9" in error_msg and "version 8 or 7" in error_msg:
            raise ValueError(
                "Version mismatch: elasticsearch-py 9.x is incompatible with Elasticsearch 8.x. "
                "Please install elasticsearch-py 8.x: pip install 'elasticsearch>=8.0.0,<9.0.0'"
            ) from e
        raise ConnectionError(f"Failed to connect to Elasticsearch at {host}: {e}") from e
    
    return client


def search_keyword(
    client: Elasticsearch, index_name: str, keyword: str, limit: int
) -> list[dict[str, object]]:
    response = client.search(
        index=index_name,
        query={"term": {"unique_keywords": keyword.lower()}},
        size=limit,
    )
    return response.get("hits", {}).get("hits", [])


def main() -> None:
    args = parse_args()
    if not args.host:
        raise ValueError(
            "Elasticsearch host is required. Set ELASTICSEARCH_ENDPOINT or pass --host."
        )
    if not args.index:
        raise ValueError(
            "Elasticsearch index is required. Set ELASTICSEARCH_INDEX or pass --index."
        )
    client = connect(args.host, args.api_key)
    hits = search_keyword(client, args.index, args.keyword, args.limit)

    if not hits:
        print(f"Keyword '{args.keyword}' not found in index '{args.index}'")
        return

    print(f"Keyword '{args.keyword}' found in {len(hits)} documents:\n")
    for idx, hit in enumerate(hits, 1):
        source = hit.get("_source", {})
        score = hit.get("_score", 0.0)
        
        episode_id = source.get("episode_id", "N/A")
        episode_title = source.get("episode_title", "N/A")
        podcast_title = source.get("podcast_title", "N/A")
        podcast_author = source.get("podcast_author", "N/A")
        podcast_image = source.get("podcast_image_url", "N/A")
        pub_date = source.get("episode_pub_date", "N/A")
        duration = source.get("episode_duration_seconds", 0)
        content = source.get("content", "")
        
        print(f"[{idx}] Episode: {episode_title}")
        print(f"    Podcast: {podcast_title}")
        if podcast_author and podcast_author != "N/A":
            print(f"    Author: {podcast_author}")
        if podcast_image and podcast_image != "N/A":
            print(f"    Image: {podcast_image}")
        print(f"    Published: {pub_date}")
        if duration:
            print(f"    Duration: {duration // 60}m {duration % 60}s")
        print(f"    Episode ID: {episode_id}")
        print(f"    Score: {score:.2f}")
        
        if content:
            content_snippet = content[:300].replace("\n", " ")
            if len(content) > 300:
                content_snippet += "..."
            print(f"    Content: {content_snippet}")
        
        print()


if __name__ == "__main__":
    main()
