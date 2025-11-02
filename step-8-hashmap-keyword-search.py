#!/usr/bin/env python3

import argparse
import hashlib
import json
import re
from pathlib import Path

TOKEN_PATTERN = re.compile(r"\b\w+\b")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hash unique words from text files and build a hashmap."
    )
    parser.add_argument(
        "--input-dir",
        default="output_speach_to_text",
        help="Directory containing .txt files to process.",
    )
    parser.add_argument(
        "--output",
        help="Location to write the resulting hashmap as JSON. Defaults to INPUT_DIR/word_hash_map.json.",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Text encoding to use when reading files.",
    )
    return parser.parse_args()


def collect_indices(
    directory: Path, encoding: str
) -> tuple[dict[str, dict[str, object]], dict[str, list[str]]]:
    word_data: dict[str, dict[str, object]] = {}
    file_index: dict[str, list[str]] = {}
    txt_files = sorted(directory.glob("*.txt"))

    if not txt_files:
        raise FileNotFoundError(f"No .txt files found in {directory}")

    for file_path in txt_files:
        text = file_path.read_text(encoding=encoding)
        tokens = set(TOKEN_PATTERN.findall(text.lower()))
        if not tokens:
            continue
        relative_name = str(file_path.relative_to(directory))
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
            entry["files"].add(relative_name)
            hashes_for_file.add(token_hash)

        if hashes_for_file:
            file_index[relative_name] = sorted(hashes_for_file)

    if not word_data:
        raise ValueError(f"No words found in files under {directory}")

    return word_data, file_index


def hash_word(word: str) -> str:
    return hashlib.sha256(word.encode("utf-8")).hexdigest()


def build_indices(
    word_data: dict[str, dict[str, object]],
    file_index: dict[str, list[str]],
) -> dict[str, dict[str, object] | dict[str, list[str]]]:
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


def write_hash_map(hash_map: dict[str, dict[str, object] | dict[str, list[str]]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(hash_map, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_path = input_dir / "word_hash_map.json"

    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Input directory does not exist: {input_dir}")

    word_data, file_index = collect_indices(input_dir, args.encoding)
    combined_map = build_indices(word_data, file_index)
    write_hash_map(combined_map, output_path)

    print(f"Processed {len(word_data)} unique words across {len(file_index)} files")
    print(f"Hashmap written to {output_path}")


if __name__ == "__main__":
    main()

