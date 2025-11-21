#!/usr/bin/env python3

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

TOKEN_PATTERN = re.compile(r"\b\w+\b")


def hash_word(word: str) -> str:
    """Hash a word using SHA256."""
    return hashlib.sha256(word.encode("utf-8")).hexdigest()


def load_hash_map(json_path: Path) -> dict:
    """
    Load hash map from JSON file.
    
    Returns:
        dict with keys:
            word_index: dict mapping hash -> {word, files}
            file_index: dict mapping filename -> sorted list of hashes
    """
    if not json_path.exists():
        raise FileNotFoundError(f"Hash map file not found: {json_path}")
    
    print(f"Loading hash map from {json_path}...")
    data = json.loads(json_path.read_text(encoding="utf-8"))
    
    word_index_size = len(data.get("word_index", {}))
    file_index_size = len(data.get("file_index", {}))
    print(f"Loaded {word_index_size} words and {file_index_size} files")
    
    return data


def search_keyword(
    word: str,
    hash_map: dict
) -> list[str]:
    """
    Search for a keyword in the hash map.
    
    Args:
        word: The word to search for
        hash_map: The loaded hash map dictionary
        
    Returns:
        List of filenames containing the word (empty list if not found)
    """
    # Normalize word to lowercase
    word = word.lower()
    
    # Hash the word
    word_hash = hash_word(word)
    
    # Look up in word_index (O(1) dictionary lookup)
    word_index = hash_map.get("word_index", {})
    entry = word_index.get(word_hash)
    
    if entry is None:
        return []
    
    # Verify the word matches (safety check)
    if entry.get("word") != word:
        # Hash collision? Shouldn't happen with SHA256, but check anyway
        return []
    
    # Return list of files
    return entry.get("files", [])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search for keywords in locally stored hash map (O(1) lookup)."
    )
    parser.add_argument(
        "--word",
        required=True,
        help="Keyword to search for.",
    )
    parser.add_argument(
        "--hash-map",
        default="output_speach_to_text/word_hash_map.json",
        help="Path to hash map JSON file. Defaults to output_speach_to_text/word_hash_map.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    # Load hash map
    hash_map_path = Path(args.hash_map).expanduser().resolve()
    hash_map = load_hash_map(hash_map_path)
    
    # Search for keyword
    files = search_keyword(args.word, hash_map)
    
    # Print results
    if files:
        print(f"\nFound '{args.word}' in {len(files)} file(s):")
        for filename in files:
            print(f"  - {filename}")
    else:
        print(f"\n'{args.word}' not found in any files.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

