"""
fetch_latest_prices.py

Update one or more mode-specific CSV files in the same directory as this script by filling the
latest CLOSE price (LC) from Yahoo Finance.

- The script looks for these CSV files (if present) and updates them:
    * us.csv
    * sg.csv
    * cc.csv
  If a file does not exist, it is skipped.

- CSV requirements:
  * Must have a header row
  * Must contain at least these columns: symbol, price
  * "symbol" cells are pre-filled; "price" may be blank or filled (will be overwritten)
  * Duplicate symbols are allowed; all matching rows will be filled

- Yahoo symbol mapping:
  * us: used as-is (e.g., AAPL)
  * sg: appends ".SI" if missing (e.g., D05 -> D05.SI)
  * cc: appends "-USD" if missing (e.g., BTC -> BTC-USD)

Usage:
  python fetch_latest_prices.py
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import zlib
import http.cookiejar as cookielib
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from tqdm import tqdm


# =======================
# CSV input path (top-level)
# =======================
# The script will read and overwrite "<mode>.csv" in the SAME directory as this script.
INPUT_CSV_TEMPLATE = "{mode}.csv"


# =======================
# Yahoo endpoints / HTTP
# =======================
YF_HOME = "https://finance.yahoo.com/"
YF_QUOTE_PAGE = "https://finance.yahoo.com/quote/{symbol}?p={symbol}"

# 1 year of daily bars; we take the latest non-None close from the returned series
YF_CHART_1Y_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?"
    "interval=1d&range=1y&includeAdjustedClose=true"
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_CJ = cookielib.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_CJ))


def _decompress_and_decode(resp, data: bytes) -> str:
    enc = (resp.headers.get("Content-Encoding") or "").lower()
    if enc == "gzip" or (len(data) > 2 and data[:2] == b"\x1f\x8b"):
        data = gzip.decompress(data)
    elif enc == "deflate":
        data = zlib.decompress(data, -zlib.MAX_WBITS)
    return data.decode("utf-8", errors="replace")


def http_get_json(url: str, timeout: int = 20) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.8",
            "Connection": "keep-alive",
            "Referer": "https://finance.yahoo.com/",
            "Origin": "https://finance.yahoo.com",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        },
    )
    with _OPENER.open(req, timeout=timeout) as resp:
        data = resp.read()
        text = _decompress_and_decode(resp, data)
        return json.loads(text)


def http_get_text(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.8",
            "Connection": "keep-alive",
            "Referer": "https://finance.yahoo.com/",
            "Origin": "https://finance.yahoo.com",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        },
    )
    with _OPENER.open(req, timeout=timeout) as resp:
        data = resp.read()
        return _decompress_and_decode(resp, data)


def warm_up_cookies(symbol_for_visit: str) -> None:
    """
    Best-effort warm-up to reduce Yahoo blocking by first visiting:
      - Yahoo Finance home
      - One quote page
    """
    try:
        _ = http_get_text(YF_HOME)
        time.sleep(0.2)
        _ = http_get_text(YF_QUOTE_PAGE.format(symbol=symbol_for_visit))
        time.sleep(0.2)
    except Exception:
        # silent warmup; fetch calls will still try
        pass


# =======================
# Symbol normalization
# =======================
def ensure_si(ticker: str) -> str:
    t = ticker.strip().upper()
    return t if t.endswith(".SI") else f"{t}.SI"


def ensure_cc(ticker: str) -> str:
    t = ticker.strip().upper()
    return t if t.endswith("-USD") else f"{t}-USD"


def normalize_symbol(mode: str, raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if mode == "sg":
        return ensure_si(s)
    if mode == "cc":
        return ensure_cc(s)
    # us
    return s.upper()


# =======================
# Price fetching
# =======================
def latest_non_none(arr: List[Optional[float]]) -> float:
    for x in reversed(arr):
        if x is not None:
            return float(x)
    raise ValueError("No non-None values found")


def fetch_latest_close(symbol: str, timeout: int = 20) -> float:
    payload = http_get_json(YF_CHART_1Y_URL.format(symbol=symbol), timeout=timeout)
    result = payload.get("chart", {}).get("result", []) or []
    if not result:
        err = payload.get("chart", {}).get("error", {}) or {}
        msg = err.get("description") or "No chart result"
        raise ValueError(msg)

    r0 = result[0]
    ind = (r0.get("indicators", {}) or {})
    quote = (ind.get("quote", [{}]) or [{}])[0]
    closes = quote.get("close") or []
    if not closes:
        raise ValueError("No close series returned")

    return latest_non_none(closes)


def format_price_for_csv(x: float) -> str:
    # Significant digits works well across stocks / crypto without forcing trailing zeros.
    return f"{x:.12g}"


# =======================
# CSV read/write
# =======================
def _find_col(fieldnames: List[str], target: str) -> Optional[str]:
    """
    Find a column name case-insensitively. Returns the actual header key.
    """
    t = target.strip().lower()
    for fn in fieldnames:
        if (fn or "").strip().lower() == t:
            return fn
    return None


def read_csv_rows(path: Path) -> Tuple[List[dict], List[str], str, str]:
    """
    Returns:
      rows, original_fieldnames, symbol_col_key, price_col_key
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")

        fieldnames = list(reader.fieldnames)
        symbol_key = _find_col(fieldnames, "symbol")
        price_key = _find_col(fieldnames, "price")

        if not symbol_key or not price_key:
            raise ValueError("CSV must contain columns: symbol, price")

        rows = list(reader)
        return rows, fieldnames, symbol_key, price_key


def write_csv_rows(path: Path, fieldnames: List[str], rows: List[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# =======================
# Main
# =======================
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Fill latest close prices into us.csv / sg.csv / cc.csv (whichever exist)."
    )
    ap.add_argument("--sleep", type=float, default=0.3, help="Seconds to sleep between Yahoo requests.")
    ap.add_argument("--timeout", type=int, default=20, help="HTTP timeout (seconds).")
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent

    any_processed = False

    for mode in ["us", "sg", "cc"]:
        csv_path = script_dir / INPUT_CSV_TEMPLATE.format(mode=mode)

        if not csv_path.exists():
            continue

        any_processed = True

        try:
            rows, fieldnames, symbol_key, price_key = read_csv_rows(csv_path)
        except Exception as e:
            print(f"[WARN] Skipping {csv_path}: {e}", file=sys.stderr)
            continue

        # Normalize symbols from CSV (keep row order; duplicates allowed)
        norm_syms: List[str] = []
        for r in rows:
            raw = (r.get(symbol_key) or "").strip()
            norm = normalize_symbol(mode, raw)
            norm_syms.append(norm)

        unique_syms = [s for s in dict.fromkeys(norm_syms) if s]  # de-dupe, keep order

        if not unique_syms:
            print(f"[WARN] No symbols found in {csv_path}; skipping.", file=sys.stderr)
            continue

        # Warm up using first symbol (best-effort)
        warm_up_cookies(unique_syms[0])

        # Fetch latest close for each unique symbol once
        price_map: Dict[str, Optional[float]] = {}
        failures: List[Tuple[str, str]] = []

        for sym in tqdm(unique_syms, desc=f"Fetching ({mode})", unit="symbol"):
            try:
                px = fetch_latest_close(sym, timeout=args.timeout)
                price_map[sym] = px
            except (urllib.error.HTTPError, urllib.error.URLError, ValueError, json.JSONDecodeError) as e:
                price_map[sym] = None
                failures.append((sym, str(e)))
            finally:
                time.sleep(args.sleep)

        # Fill every row (including duplicates); overwrite any existing price
        filled = 0
        skipped = 0
        for r, sym in zip(rows, norm_syms):
            if not sym:
                r[price_key] = ""
                skipped += 1
                continue

            px = price_map.get(sym)
            if px is None:
                r[price_key] = ""
                skipped += 1
            else:
                r[price_key] = format_price_for_csv(px)
                filled += 1

        # Overwrite the CSV
        write_csv_rows(csv_path, fieldnames, rows)

        print(f"[INFO] Updated: {csv_path}")
        print(f"[INFO] Rows filled: {filled}; rows skipped/blank: {skipped}")
        if failures:
            print(f"[WARN] Failed symbols: {len(failures)}", file=sys.stderr)
            for sym, msg in failures[:20]:
                print(f"[WARN] {sym}: {msg}", file=sys.stderr)
            if len(failures) > 20:
                print(f"[WARN] (showing first 20 only)", file=sys.stderr)

    if not any_processed:
        print("[INFO] No us.csv / sg.csv / cc.csv found; nothing to do.")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
