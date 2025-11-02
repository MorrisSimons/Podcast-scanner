#!/usr/bin/env python3

"""Upload transcript text files to Elasticsearch with unique keyword metadata."""

import argparse
import os
import re
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from elasticsearch import Elasticsearch, helpers

TOKEN_PATTERN = re.compile(r"\b\w+\b")


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
