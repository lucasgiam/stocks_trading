"""
extract_symbols.py

Usage:
  python extract_symbols.py --input path/to/file.csv

Reads the CSV file, takes all values from column A (first column),
and prints them space-separated, like:

  S1 S2 S3 ...
"""

import argparse
import csv
import sys
from pathlib import Path


def parse_args():
    ap = argparse.ArgumentParser(description="Extract symbols from column A of a CSV.")
    ap.add_argument(
        "--input",
        required=True,
        help="Path to the input CSV file.",
    )
    return ap.parse_args()


def main():
    args = parse_args()
    csv_path = Path(args.input)

    if not csv_path.is_file():
        print(f"ERROR: File not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    symbols = []

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            first_row = True
            for row in reader:
                if not row:
                    continue
                val = row[0].strip()
                if not val:
                    continue

                # Heuristic: Skip common header names in the first row
                if first_row and val.lower() in {"symbol", "code", "ticker", "stock"}:
                    first_row = False
                    continue

                first_row = False
                symbols.append(val)
    except Exception as e:
        print(f"ERROR: Failed to read CSV: {e}", file=sys.stderr)
        sys.exit(1)

    print(" ".join(symbols))


if __name__ == "__main__":
    main()
