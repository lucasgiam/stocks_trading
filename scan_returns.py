"""
scan_returns.py

Scan SGX, US, or crypto tickers on Yahoo and compute total price return
(excluding dividends) over several trailing windows:
- YTD   : latest close vs. last close on/before 31 Dec of the previous year (simple return)
- 1Y    : latest close vs. last close on/before this date 1 year ago (simple return)
- 3Y    : latest close vs. last close on/before this date 3 years ago (annualized, CAGR)
- 5Y    : latest close vs. last close on/before this date 5 years ago (annualized, CAGR)
- 10Y   : latest close vs. last close on/before this date 10 years ago (annualized, CAGR)
- 20Y   : latest close vs. last close on/before this date 20 years ago (annualized, CAGR)

A single chart is fetched per symbol covering the last ~21 years (via an
explicit period1/period2 request, since Yahoo's range=max/10y+ keywords
silently downsample very long lookbacks to monthly bars), and all 6 windows
are derived from that one daily series by locating the closest available
trading day on/before each target date. If a symbol's history doesn't reach
far enough back for a given window, that cell is 'nan'.

Usage examples:
  python scan_returns.py --mode us --symbols AAPL MSFT NVDA --sort_by 5y
  python scan_returns.py --mode sg --symbols auto --sort_by ytd
  python scan_returns.py --mode cc --symbols BTC ETH SOL --sort_by none

Notes:
- --mode selects:
    'sg' for SGX (codes like 'D05', 'C6L'; mapped to Yahoo by appending '.SI'),
    'us' for US stocks (codes like 'AAPL', 'GOOG'; used as-is),
    'cc' for cryptocurrencies (codes like 'BTC', 'ETH'; mapped to Yahoo by appending '-USD').
- --symbols takes space-separated codes (no quotes), or 'auto' to load from all_<mode>_stocks.txt.
- --exclude removes the specified symbols from being processed (normalization by mode is applied).
- --sort_by: 'ytd' | '1y' | '3y' | '5y' | '10y' | '20y' sorts descending (highest return
  first, missing/'nan' values sorted last); 'none' keeps scan order (default).
"""

from __future__ import annotations

import argparse
import bisect
import sys
import time
from datetime import datetime, timezone

from tqdm import tqdm

from yf_common import (
    build_symbol_list,
    fetch_chart,
    fmtf,
    fmt_price,
    get_name_map,
    is_finite,
    load_auto_symbols,
    warm_up_cookies_and_crumb,
)

# ---------- trailing windows (years); 1y is simple, 3y/5y/10y/20y are annualized ----------
ALL_YEAR_WINDOWS = [1, 3, 5, 10, 20]

SORT_KEY_TO_COLUMN = {
    "ytd": "YTD%",
    "1y":  "1Y%",
    "3y":  "3Y%",
    "5y":  "5Y%",
    "10y": "10Y%",
    "20y": "20Y%",
}


def to_utc_date(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).date()


def years_ago(today, n):
    """`today` shifted back by n calendar years, clamping 29 Feb -> 28 Feb."""
    try:
        return today.replace(year=today.year - n)
    except ValueError:
        return today.replace(year=today.year - n, day=28)


def build_date_close_series(chart):
    """Chronological (date, close) pairs for days with a valid close."""
    timestamps = chart["timestamps"]
    closes     = chart["close"]
    n = min(len(timestamps), len(closes))
    pairs = []
    for i in range(n):
        c = closes[i]
        if c is None or not is_finite(c):
            continue
        pairs.append((to_utc_date(timestamps[i]), c))
    pairs.sort(key=lambda p: p[0])
    return pairs


def close_on_or_before(dates, closes, target_date):
    """Most recent close with date <= target_date, or None if none exists."""
    idx = bisect.bisect_right(dates, target_date) - 1
    if idx < 0:
        return None
    return closes[idx]


def compute_returns(chart):
    """Return dict with LC and YTD%/1Y%/3Y%/5Y%/10Y%/20Y% (simple or annualized)."""
    pairs = build_date_close_series(chart)
    out = {"LC": float("nan"), "YTD%": float("nan")}
    for n in ALL_YEAR_WINDOWS:
        out[f"{n}Y%"] = float("nan")

    if not pairs:
        return out

    dates  = [p[0] for p in pairs]
    closes = [p[1] for p in pairs]

    rmp = chart.get("regular_market_price")
    latest = rmp if is_finite(rmp) else closes[-1]
    out["LC"] = latest

    if not is_finite(latest):
        return out

    today = datetime.now(timezone.utc).date()

    # YTD: vs. last close on/before 31 Dec of the previous year (simple return)
    ytd_target = today.replace(year=today.year - 1, month=12, day=31)
    start = close_on_or_before(dates, closes, ytd_target)
    if is_finite(start) and start != 0:
        out["YTD%"] = 100.0 * (latest / start - 1.0)

    # N-year windows: 1Y simple; 3Y/5Y/10Y/20Y annualized (CAGR)
    for n in ALL_YEAR_WINDOWS:
        target = years_ago(today, n)
        start  = close_on_or_before(dates, closes, target)
        if is_finite(start) and start != 0 and start > 0 and latest > 0:
            if n == 1:
                out[f"{n}Y%"] = 100.0 * (latest / start - 1.0)
            else:
                out[f"{n}Y%"] = 100.0 * ((latest / start) ** (1.0 / n) - 1.0)

    return out


def main():
    ap = argparse.ArgumentParser(
        description="Scan SGX/US/crypto (Yahoo) and compute total price return over YTD/1Y/3Y/5Y/10Y/20Y."
    )
    ap.add_argument(
        "--mode",
        choices=["sg", "us", "cc"],
        required=True,
        help=(
            "Market mode: 'sg' for SGX (codes like D05, C6L; '.SI' appended), "
            "'us' for US stocks (codes like AAPL, GOOG; used as-is), "
            "'cc' for cryptocurrencies (codes like BTC, ETH; '-USD' appended)."
        ),
    )
    ap.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help=(
            "Space-separated stock/crypto codes (e.g., D05 C6L for SGX; AAPL GOOG for US; "
            "BTC ETH for crypto), or 'auto' to load from all_<mode>_stocks.txt."
        ),
    )
    ap.add_argument(
        "--exclude",
        nargs="+",
        help="Space-separated codes to exclude (mode-specific suffix optional).",
    )
    ap.add_argument(
        "--sort_by",
        choices=["ytd", "1y", "3y", "5y", "10y", "20y", "none"],
        default="none",
        help=(
            "Sort output descending by the chosen return column (missing/'nan' values sorted last); "
            "'none' keeps scan order (default)."
        ),
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.3,
        help="Seconds to sleep between requests.",
    )
    args = ap.parse_args()

    if len(args.symbols) == 1 and args.symbols[0].lower() == "auto":
        try:
            input_symbols = load_auto_symbols(args.mode)
        except (FileNotFoundError, ValueError) as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return
    else:
        input_symbols = args.symbols

    exclude_symbols = args.exclude or []
    symbols = build_symbol_list(args.mode, input_symbols, exclude_symbols)
    if not symbols:
        print("ERROR: No symbols left to scan after exclusions.", file=sys.stderr)
        return

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols[0])
    except Exception:
        pass

    name_map = get_name_map(symbols)

    # Fetch true daily bars (Yahoo's range=max silently downsamples to monthly
    # for long lookbacks) via an explicit period1/period2 request, with a
    # buffer beyond 20 years to safely cover the 20Y target date.
    period1_ts = int(time.time()) - 21 * 366 * 86400

    results = []
    for sym in tqdm(symbols, desc="Scanning", unit="symbol"):
        try:
            chart = fetch_chart(sym, period1=period1_ts)
            r = compute_returns(chart)
            if not is_finite(r["LC"]):
                raise ValueError("No usable close prices in fetched history")

            if args.mode == "sg":
                disp_code = sym.removesuffix(".SI")
            elif args.mode == "cc":
                disp_code = sym.removesuffix("-USD")
            else:
                disp_code = sym

            results.append(
                {
                    "Symbol": disp_code,
                    "Name":   name_map.get(sym, sym),
                    **r,
                }
            )
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        finally:
            time.sleep(args.sleep)

    filtered = list(results)

    # ----- Sorting -----
    if args.sort_by != "none":
        column = SORT_KEY_TO_COLUMN[args.sort_by]

        def sort_key(r):
            v = r.get(column)
            return (0, -v) if is_finite(v) else (1, float("inf"))

        filtered.sort(key=sort_key)

    print(f"\nProcessed {len(results)} valid symbols out of {len(symbols)} scanned.\n")

    header = (
        f"{'Code':<6} {'Name':<32} "
        f"{'LC':>10} {'YTD%':>8} {'1Y%':>8} {'3Y%':>8} {'5Y%':>8} {'10Y%':>8} {'20Y%':>8}"
    )
    print(header)
    print("-" * len(header))

    for r in filtered:
        print(
            f"{(r['Symbol'] or '')[:6]:<6} "
            f"{(r['Name'] or '')[:32]:<32} "
            f"{fmt_price(r['LC'], 10)} "
            f"{fmtf(r['YTD%'], 8, 2)} "
            f"{fmtf(r['1Y%'],  8, 2)} "
            f"{fmtf(r['3Y%'],  8, 2)} "
            f"{fmtf(r['5Y%'],  8, 2)} "
            f"{fmtf(r['10Y%'], 8, 2)} "
            f"{fmtf(r['20Y%'], 8, 2)}"
        )

    if filtered:
        print()
        print("Symbol list:")
        print(" ".join(r["Symbol"] for r in filtered))
        print()


if __name__ == "__main__":
    main()
