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
        default=os.getenv("ELASTICSEARCH_ENDPOINT"),
        help="Elasticsearch host URL.",
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
    client = Elasticsearch(hosts=[host], api_key=api_key) if api_key else Elasticsearch(hosts=[host])
    if not client.ping():
        raise ConnectionError(f"Failed to reach Elasticsearch at {host}")
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

    print(f"Keyword '{args.keyword}' found in {len(hits)} documents:")
    for hit in hits:
        source = hit.get("_source", {})
        score = hit.get("_score", 0.0)
        path = source.get("path", "<unknown>")
        print(f" - {path} (score={score:.2f})")


if __name__ == "__main__":
    main()
