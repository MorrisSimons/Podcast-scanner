#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional, Union

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

TOKEN_PATTERN = re.compile(r"\b\w+\b")


def connect_cassandra(
    host: str,
    username: str,
    password: str,
    keyspace: str
):
    """Connect to Cassandra cluster and return session."""
    auth = PlainTextAuthProvider(username, password)
    # Use protocol_version=5 like the working script
    cluster = Cluster(
        [host],
        auth_provider=auth,
        protocol_version=5,
    )
    session = cluster.connect(keyspace)
    return cluster, session


def hash_word(word: str) -> str:
    """Hash a word using SHA256."""
    return hashlib.sha256(word.encode("utf-8")).hexdigest()


def collect_indices_from_cassandra(
    session, encoding: str = "utf-8", batch_size: int = 50, limit: Optional[int] = None
) -> tuple[dict[str, dict[str, object]], dict[str, list[str]]]:
    """
    Read all records from transcript_files table and build indices.
    Fetches filenames first, then content in small batches to avoid CRC mismatch errors.
    
    Returns:
        tuple: (word_data, file_index) where:
            word_data: dict mapping word -> {hash, files set}
            file_index: dict mapping filename -> sorted list of hashes
    """
    word_data: dict[str, dict[str, object]] = {}
    file_index: dict[str, list[str]] = {}
    
    # Step 1: Fetch all filenames first (lightweight query)
    print("Fetching all filenames from transcript_files table...")
    filename_query = SimpleStatement("SELECT filename FROM transcript_files", fetch_size=1000)
    filename_result = session.execute(filename_query)
    
    all_filenames = [row.filename for row in filename_result]
    if not all_filenames:
        raise ValueError("No records found in transcript_files table")
    
    # Limit files if specified (for testing)
    if limit:
        all_filenames = all_filenames[:limit]
        print(f"Limiting to first {limit} files for testing...")
    
    print(f"Found {len(all_filenames)} files. Fetching content in batches of {batch_size}...")
    
    # Step 2: Fetch content in small batches to avoid CRC mismatch with large text fields
    prepared_query = session.prepare("SELECT filename, content FROM transcript_files WHERE filename = ?")
    
    file_count = 0
    with tqdm(total=len(all_filenames), desc="Processing files", unit="file") as pbar:
        for i in range(0, len(all_filenames), batch_size):
            batch_filenames = all_filenames[i:i + batch_size]
            
            # Fetch content for this batch
            for filename in batch_filenames:
                try:
                    result = session.execute(prepared_query, (filename,))
                    row = result.one()
                    
                    if not row or not row.content:
                        pbar.update(1)
                        continue
                    
                    content = row.content
                    file_count += 1
                    
                    # Tokenize content
                    tokens = set(TOKEN_PATTERN.findall(content.lower()))
                    if not tokens:
                        pbar.update(1)
                        continue
                    
                    hashes_for_file: set[str] = set()
                    
                    for token in tokens:
                        token_hash = hash_word(token)
                        entry = word_data.setdefault(
                            token,
                            {
                                "hash": token_hash,
                                "files": set(),
                            },
                        )
                        entry["files"].add(filename)
                        hashes_for_file.add(token_hash)
                    
                    if hashes_for_file:
                        file_index[filename] = sorted(hashes_for_file)
                    
                    pbar.update(1)
                except Exception as e:
                    print(f"\nWARNING: Error processing {filename}: {e}")
                    pbar.update(1)
                    continue
    
    if file_count == 0:
        raise ValueError("No files with content found in transcript_files table")
    
    if not word_data:
        raise ValueError("No words found in transcript_files")
    
    print(f"\nProcessed {file_count} files with content")
    return word_data, file_index


def build_indices(
    word_data: dict[str, dict[str, object]],
    file_index: dict[str, list[str]],
) -> dict[str, Union[dict[str, object], dict[str, list[str]]]]:
    """
    Build final index structures from word_data and file_index.
    
    Returns:
        dict with keys:
            word_index: dict mapping hash -> {word, files}
            file_index: dict mapping filename -> sorted list of hashes
    """
    word_index: dict[str, dict[str, object]] = {}
    for word in sorted(word_data):
        entry = word_data[word]
        word_index[entry["hash"]] = {
            "word": word,
            "files": sorted(entry["files"]),
        }
    
    return {
        "word_index": word_index,
        "file_index": dict(sorted(file_index.items())),
    }


def write_hash_map(
    hash_map: dict[str, Union[dict[str, object], dict[str, list[str]]]], 
    output_path: Path
) -> None:
    """Write hash map to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Use ensure_ascii=False to preserve Unicode characters (e.g., Swedish ö, ä, å)
    output_path.write_text(json.dumps(hash_map, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build hash-based indices from Cassandra transcript_files and save to local JSON."
    )
    parser.add_argument(
        "--output",
        default="output_speach_to_text/word_hash_map.json",
        help="Path to output JSON file. Defaults to output_speach_to_text/word_hash_map.json",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Text encoding to use when reading files.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of files to fetch content for in each batch (default: 50). Lower if you get CRC errors.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of files to process (useful for testing).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
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
    
    try:
        # Collect indices from Cassandra
        word_data, file_index = collect_indices_from_cassandra(session, args.encoding, args.batch_size, args.limit)
        
        # Build final indices
        combined_map = build_indices(word_data, file_index)
        
        # Write to JSON
        output_path = Path(args.output).expanduser().resolve()
        write_hash_map(combined_map, output_path)
        
        print(f"\nProcessed {len(word_data)} unique words across {len(file_index)} files")
        print(f"Hashmap written to {output_path}")
        
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

