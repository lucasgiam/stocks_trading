#!/usr/bin/env python3
"""
dedupe_symbols.py

Usage:
  python dedupe_symbols.py --input path/to/symbols.txt

The input text file should contain symbols separated by whitespace, e.g.:

  AAPL TSLA TSLA BTC AAPL

The script will:
1. Scan for duplicates and print them in the format:
     SYMBOL x2 / x3 / x4 ...
2. Print the de-duplicated symbols (each unique symbol kept once,
   in order of first appearance), space-separated, to stdout.
"""

import argparse
import sys
from pathlib import Path
from collections import Counter


def parse_args():
    ap = argparse.ArgumentParser(
        description="Remove duplicate symbols from a whitespace-separated list."
    )
    ap.add_argument(
        "--input",
        required=True,
        help="Path to the input .txt file containing whitespace-separated symbols.",
    )
    return ap.parse_args()


def main():
    args = parse_args()
    txt_path = Path(args.input)

    if not txt_path.is_file():
        print(f"ERROR: File not found: {txt_path}", file=sys.stderr)
        sys.exit(1)

    try:
        text = txt_path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"ERROR: Failed to read file: {e}", file=sys.stderr)
        sys.exit(1)

    # Split on any whitespace (spaces, newlines, tabs, etc.)
    symbols = text.split()
    if not symbols:
        print("No symbols found in file.", file=sys.stderr)
        sys.exit(0)

    # Count occurrences
    counts = Counter(symbols)

    # Count how many unique symbols have duplicates
    dup_symbol_count = sum(1 for cnt in counts.values() if cnt > 1)

    if dup_symbol_count == 0:
        print("No duplicates found.")
    else:
        print(f"Found {dup_symbol_count} symbols with duplicates:")

        # Now print each symbol's duplicate count
        for sym, cnt in counts.items():
            if cnt > 1:
                print(f"{sym} x{cnt}")

    # Build de-duplicated list (preserve first occurrence order)
    seen = set()
    unique_symbols = []
    for sym in symbols:
        if sym not in seen:
            seen.add(sym)
            unique_symbols.append(sym)

    # Print a blank line then the deduped spaced symbols
    print()
    print("Deduplicated symbols:")
    print(" ".join(unique_symbols))


if __name__ == "__main__":
    main()
