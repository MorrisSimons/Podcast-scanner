#!/usr/bin/env python3

import json
from pathlib import Path


def find_most_common_word(json_path: Path) -> None:
    """
    Find the most common word (appears in most files) in the hash map.
    """
    print(f"Loading hash map from {json_path}...")
    data = json.loads(json_path.read_text(encoding="utf-8"))
    
    word_index = data.get("word_index", {})
    print(f"Processing {len(word_index)} words...")
    
    most_common_word = None
    most_common_count = 0
    
    for word_hash, entry in word_index.items():
        word = entry.get("word", "")
        files = entry.get("files", [])
        file_count = len(files)
        
        if file_count > most_common_count:
            most_common_count = file_count
            most_common_word = word
    
    if most_common_word is not None:
        print(f"\nMost common word: '{most_common_word}'")
        print(f"Appears in {most_common_count} file(s)")
    else:
        print("\nNo words found in hash map.")


if __name__ == "__main__":
    dict_path = Path(__file__).parent.parent / "step-8-space_complexity_output" / "Dict_3000.json"
    find_most_common_word(dict_path)

