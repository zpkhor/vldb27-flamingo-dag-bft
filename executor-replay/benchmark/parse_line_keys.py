#!/usr/bin/env python3
"""Parse keys from log-file lines containing dictionaries and print them in order."""
# python parse_line_keys.py logs/primary-3.log, should print True as each validator client sends a continuous range of account_id requests
import ast
import argparse
from pathlib import Path


def extract_keys(line: str) -> set:
    start = line.find("{")
    end = line.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("No dictionary found in input line")

    dict_text = line[start : end + 1]
    parsed = ast.literal_eval(dict_text)
    if not isinstance(parsed, dict):
        raise ValueError("Parsed value is not a dictionary")

    return set(parsed.keys())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract dictionary keys from a log file and print sorted keys"
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Path to a log file.",
    )
    parser.add_argument(
        "--contains",
        default="Header account_counts:",
        help="Only parse lines containing this text.",
    )
    args = parser.parse_args()

    all_keys = set()
    with args.file.open("r", encoding="utf-8") as f:
        for line in f:
            if args.contains not in line:
                continue
            try:
                all_keys.update(extract_keys(line))
            except ValueError:
                continue

    print(sorted(all_keys) == list(range(min(all_keys), max(all_keys) + 1)))


if __name__ == "__main__":
    main()
