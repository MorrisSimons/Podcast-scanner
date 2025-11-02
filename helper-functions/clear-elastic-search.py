#!/usr/bin/env python3

"""Clear Elasticsearch index - removes all test data."""

import argparse
import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clear/delete Elasticsearch index."
    )
    parser.add_argument(
        "--index",
        default=os.getenv("ELASTICSEARCH_INDEX", "podcast-transcripts"),
        help="Elasticsearch index name to delete.",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("ELASTICSEARCH_ENDPOINT"),
        help="Elasticsearch host URL.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ELASTICSEARCH_APIKEY"),
        help="Elasticsearch API key.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.host:
        raise ValueError(
            "Elasticsearch host is required. Set ELASTICSEARCH_ENDPOINT or pass --host."
        )

    client = (
        Elasticsearch(hosts=[args.host], api_key=args.api_key)
        if args.api_key
        else Elasticsearch(hosts=[args.host])
    )

    if not client.ping():
        raise ConnectionError(f"Failed to reach Elasticsearch at {args.host}")

    if not client.indices.exists(index=args.index):
        print(f"Index '{args.index}' does not exist. Nothing to clear.")
        return

    client.indices.delete(index=args.index)
    print(f"Deleted index '{args.index}' at {args.host}")


if __name__ == "__main__":
    main()