"""
scan_stocks.py

Scan SGX, US, crypto, or index tickers on Yahoo and compute:
- LC (latest close)
- MA5   (5-day moving average)
- MA20  (20-day moving average)
- MA50  (50-day moving average)
- MA100 (100-day moving average)
- MA200 (200-day moving average)
- ΔLC% = 100 * (LC - MA20) / MA20
- Z-ATR = (LC - MA20) / ATR20
- ATR5   (5-period Average True Range, simple average of last 5 TR values)
- ATR20  (20-period Average True Range, simple average of last 20 TR values)
- ATR200 (200-period Average True Range, simple average of last 200 TR values)
- ATR% = 100 * ATR20 / LC

Usage example:
  python scan_stocks.py --mode sg --symbols CC3 G13 N2IU C6L --delta_thres 0 --z_thres 0 --atr_thres 3 --sort_by delta
  python scan_stocks.py --mode us --symbols AAPL GOOG MSFT NVDA --delta_thres 0 --z_thres 0 --atr_thres 3 --sort_by atr
  python scan_stocks.py --mode cc --symbols BTC ETH SOL --delta_thres 0 --z_thres 0 --atr_thres 3 --sort_by z
  python scan_stocks.py --mode id --symbols ^STI ^DJI ^IXIC ^GSPC --delta_thres 0 --z_thres 0 --atr_thres 3 --sort_by delta

Notes:
- --mode selects:
    'sg' for SGX (codes like 'D05', 'C6L'; mapped to Yahoo by appending '.SI'),
    'us' for US stocks (codes like 'AAPL', 'GOOG'; used as-is),
    'cc' for cryptocurrencies (codes like 'BTC', 'ETH'; mapped to Yahoo by appending '-USD'),
    'id' for indexes (codes like '^STI', '^DJI'; used as-is for Yahoo, but '^' stripped in display).
- --symbols takes space-separated codes (no quotes), or 'auto' to load from all_<mode>_stocks.txt.
  For --mode id, if --symbols is omitted/empty, the default symbols used are: ^STI, ^DJI, ^IXIC, ^GSPC.
- --delta_thres:
    * if X <= 0, keep rows where Delta% <= X
    * if X > 0, keep rows where Delta% > X
    * or set to 'z' to use per-record Delta% ≤ that record's Z-ATR.
- --z_thres:
    * if X <= 0, keep rows where Z-ATR <= X
    * if X > 0, keep rows where Z-ATR > X.
- --atr_thres keeps only rows where ATR-LC% >= atr_thres.
- --sort_by controls sorting of the final table:
    * 'delta': sort by ΔLC%; if delta_thres <= 0 or not specified → increasing (most negative first),
               if delta_thres > 0 → decreasing (most positive first).
    * 'z':     sort by Z-ATR; if z_thres <= 0 or not specified → increasing (most negative first),
               if z_thres > 0 → decreasing (most positive first).
    * 'atr':   sort by ATR-LC% (ATR20/LC), always in decreasing order (largest first).
- --reg_filter, when set, applies a long-term regime filter: keep only rows where LC > MA200.
- --exclude removes the specified symbols from being processed (normalization by mode is applied).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
import urllib.error
import gzip
import zlib
import http.cookiejar as cookielib
import re
import math
from collections import Counter
from tqdm import tqdm

# Yahoo endpoints
YF_HOME = "https://finance.yahoo.com/"
YF_GET_CRUMB = "https://query1.finance.yahoo.com/v1/test/getcrumb"
YF_QUOTE_PAGE = "https://finance.yahoo.com/quote/{symbol}?p={symbol}"
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}&lang=en-US&region=US"
YF_QUOTE_URL_ALT = "https://query2.finance.yahoo.com/v7/finance/quote?symbols={symbols}&lang=en-US&region=US"
YF_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search?q={symbol}&quotesCount=1"

# 1 year of daily bars; include adjusted close
YF_CHART_1Y_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?"
    "interval=1d&range=1y&includeAdjustedClose=true"
)

# quoteSummary URLs (currently not used in output, kept for possible future use)
YF_SUMMARY_URL_Q2 = (
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    "?modules=summaryDetail,price&formatted=false&lang=en-US&region=US&ssl=true&corsDomain=finance.yahoo.com"
)
YF_SUMMARY_URL_Q1 = (
    "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    "?modules=summaryDetail,price&formatted=false&lang=en-US&region=US&ssl=true&corsDomain=finance.yahoo.com"
)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
_CJ = cookielib.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_CJ))
_CRUMB = None  # filled by warm_up_cookies_and_crumb()


def _decompress_and_decode(resp, data: bytes) -> str:
    enc = (resp.headers.get("Content-Encoding") or "").lower()
    if enc == "gzip" or (len(data) > 2 and data[:2] == b"\x1f\x8b"):
        data = gzip.decompress(data)
    elif enc == "deflate":
        data = zlib.decompress(data, -zlib.MAX_WBITS)
    return data.decode("utf-8", errors="replace")


def http_get_json(url, timeout=20):
    if "{crumb}" in url:
        url = url.format(crumb=_CRUMB or "")
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


def http_get_text(url, timeout=20):
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


def warm_up_cookies_and_crumb(symbol_si_for_visit: str):
    """Make Yahoo happy; try to fetch crumb token."""
    global _CRUMB
    try:
        _ = http_get_text(YF_HOME)
        time.sleep(0.3)
        _ = http_get_text(YF_QUOTE_PAGE.format(symbol=symbol_si_for_visit))
        time.sleep(0.3)
        try:
            crumb_text = http_get_text(YF_GET_CRUMB).strip()
            if crumb_text and len(crumb_text) < 64:
                _CRUMB = crumb_text
        except Exception as e:
            print(f"[WARN] crumb fetch failed: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] warm-up failed: {e}", file=sys.stderr)


def ensure_si(ticker: str) -> str:
    t = ticker.strip().upper()
    return t if t.endswith(".SI") else f"{t}.SI"


def ensure_cc(ticker: str) -> str:
    """Normalize crypto tickers to Yahoo's '-USD' format."""
    t = ticker.strip().upper()
    return t if t.endswith("-USD") else f"{t}-USD"


def ensure_idx(ticker: str) -> str:
    """Normalize index tickers to Yahoo's '^CODE' format."""
    t = ticker.strip().upper()
    return t if t.startswith("^") else f"^{t}"


def try_quote_names(symbols_si):
    """Fast path: quote endpoint for names."""
    name_map = {s: s for s in symbols_si}
    try:
        payload = http_get_json(YF_QUOTE_URL.format(symbols=",".join(symbols_si)))
        for q in payload.get("quoteResponse", {}).get("result", []):
            sym = q.get("symbol", "")
            nm = (
                q.get("shortName")
                or q.get("longName")
                or q.get("displayName")
                or sym
            )
            name_map[sym] = nm
    except Exception:
        # keep silent to match original behavior
        pass
    return name_map


def try_search_name(symbol_si):
    try:
        p = http_get_json(YF_SEARCH_URL.format(symbol=symbol_si))
        quotes = p.get("quotes", []) or []
        if quotes:
            return (
                quotes[0].get("shortname")
                or quotes[0].get("longname")
                or symbol_si
            )
    except Exception:
        pass
    return symbol_si


def get_name_map(symbols_si):
    nm = try_quote_names(symbols_si)
    for s in symbols_si:
        if not nm.get(s) or nm.get(s) == s:
            nm[s] = try_search_name(s)
    return nm


def fetch_chart_1y(symbol_si):
    """Return dict with arrays: open, high, low, close, volume (may contain None)."""
    payload = http_get_json(YF_CHART_1Y_URL.format(symbol=symbol_si))
    result = payload.get("chart", {}).get("result", []) or []
    if not result:
        raise ValueError("No chart result")
    r0 = result[0]
    ind = (r0.get("indicators", {}) or {})
    quote = (ind.get("quote", [{}]) or [{}])[0]
    return {
        "open": quote.get("open") or [],
        "high": quote.get("high") or [],
        "low": quote.get("low") or [],
        "close": quote.get("close") or [],
        "volume": quote.get("volume") or [],
    }


def mean(vals):
    return sum(vals) / len(vals) if vals else float("nan")


def std_pop(vals):
    n = len(vals)
    if n == 0:
        return float("nan")
    m = mean(vals)
    var = sum((x - m) ** 2 for x in vals) / n
    return math.sqrt(var)


def ma_last(closes_valid, n):
    """Return last simple moving average value over window n (or NaN if insufficient)."""
    if len(closes_valid) < n:
        return float("nan")
    window = closes_valid[-n:]
    return mean(window)


def compute_atr(highs, lows, closes, window):
    """ATR(window) as simple average of the last `window` True Range values."""
    tr_list = []
    prev_close = None
    N = max(len(highs), len(lows), len(closes))
    for i in range(N):
        h = highs[i] if i < len(highs) else None
        l = lows[i] if i < len(lows) else None
        c = closes[i] if i < len(closes) else None
        if h is None or l is None:
            prev_close = c if c is not None else prev_close
            continue
        if prev_close is None:
            tr = h - l
        else:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        if isinstance(tr, (int, float)) and math.isfinite(tr):
            tr_list.append(tr)
        prev_close = c if c is not None else prev_close

    if not tr_list:
        return float("nan")
    last_n = tr_list[-window:] if len(tr_list) >= window else tr_list
    return mean(last_n)


def latest_non_none(arr):
    for x in reversed(arr):
        if x is not None:
            return x
    return float("nan")


def is_finite(x):
    return isinstance(x, (int, float)) and math.isfinite(x)


# ---------- compact one-row table ----------
def fmtf(x, w, p):
    return f"{x:>{w}.{p}f}" if is_finite(x) else f"{'nan':>{w}}"


def fmt_price(x, width=6, max_dp=3):
    if not is_finite(x):
        return f"{'nan':>{width}}"
    # Try from max_dp down to 0 dp, pick first that fits
    for dp in range(max_dp, -1, -1):
        s = f"{x:.{dp}f}"
        if len(s) <= width:
            return s.rjust(width)
    # Even integer doesn't fit -> keep most significant digits only
    s = f"{int(x):d}"
    if len(s) > width:
        s = s[:width]  # keep most significant digits
    return s.rjust(width)


def ma_stack_str(r):
    """
    Return a string like '(LC > MA5 > MA20 > MA50 > MA100 > MA200)',
    ordering LC, MA5, MA20, MA50, MA100, MA200 by actual numeric value (descending).
    """
    lc = r.get("LC")
    ma5 = r.get("MA5")
    ma20 = r.get("MA20")
    ma50 = r.get("MA50")
    ma100 = r.get("MA100")
    ma200 = r.get("MA200")

    items = [
        ("LC", lc),
        ("MA5", ma5),
        ("MA20", ma20),
        ("MA50", ma50),
        ("MA100", ma100),
        ("MA200", ma200),
    ]
    items = [(name, val) for name, val in items if is_finite(val)]
    if len(items) < 2:
        return ""

    items_sorted = sorted(items, key=lambda x: x[1], reverse=True)
    labels = [name for name, _ in items_sorted]
    return "(" + " > ".join(labels) + ")"


def main():
    ap = argparse.ArgumentParser(
        description="Scan SGX, US, crypto, or index (Yahoo) and rank by Delta% vs MA20."
    )
    ap.add_argument(
        "--mode",
        choices=["sg", "us", "cc", "id"],
        required=True,
        help=(
            "Market mode: 'sg' for SGX (codes like D05, C6L; '.SI' will be appended), "
            "'us' for US stocks (codes like AAPL, GOOG; used as-is), "
            "'cc' for cryptocurrencies (codes like BTC, ETH; '-USD' will be appended), "
            "'id' for indexes (codes like ^STI, ^DJI; used as-is for Yahoo)."
        ),
    )
    ap.add_argument(
        "--symbols",
        nargs="+",
        help=(
            "Space-separated stock/crypto/index codes "
            "(e.g., CC3 G13 for SGX; AAPL GOOG for US; BTC ETH for crypto; ^STI ^DJI for indexes). "
            "For SGX, '.SI' suffix is optional. For crypto, '-USD' suffix is optional. "
            "For indexes, '^' will be added if omitted. For --mode id with no symbols, "
            "defaults to: ^STI ^DJI ^IXIC ^GSPC."
        ),
    )
    # Accept float (as string) or the string 'z'
    ap.add_argument(
        "--delta_thres",
        default=None,
        help=(
            "Delta% filter: if X <= 0, keep rows with Delta% ≤ X; "
            "if X > 0, keep rows with Delta% > X. "
            "Use 'z' to apply per-record Delta% ≤ Z-ATR."
        ),
    )
    ap.add_argument(
        "--z_thres",
        type=float,
        default=None,
        help=(
            "Z-ATR filter: if X <= 0, keep rows with Z-ATR ≤ X; "
            "if X > 0, keep rows with Z-ATR > X."
        ),
    )
    ap.add_argument(
        "--atr_thres",
        type=float,
        default=None,
        help="Keep only rows where ATR-LC% (ATR20/LC) >= atr_thres.",
    )
    ap.add_argument(
        "--sort_by",
        choices=["delta", "atr", "z"],
        default="delta",
        help=(
            "Sort output by: 'delta' (ΔLC%), 'atr' (ATR-LC%), or 'z' (Z-ATR). "
            "For 'delta' and 'z': if threshold X <= 0 or not set → increasing (most negative first); "
            "if X > 0 → decreasing (most positive first). "
            "For 'atr': always decreasing (largest ATR-LC% first)."
        ),
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.3,
        help="Seconds to sleep between requests.",
    )
    ap.add_argument(
        "--exclude",
        nargs="+",
        help=(
            "Space-separated codes to exclude "
            "('.SI' optional for SGX; '-USD' optional for crypto; '^' optional for indexes)."
        ),
    )
    ap.add_argument(
        "--reg_filter",
        action="store_true",
        help="If set, apply long-term regime filter: keep only rows where LC > MA200.",
    )
    args = ap.parse_args()

    # For non-index modes, symbols must be provided (unless using 'auto' later).
    if not args.symbols and args.mode != "id":
        print(
            "ERROR: No symbols provided. Please supply at least one via --symbols.",
            file=sys.stderr,
        )
        return

    # Handle 'auto' mode for symbols: load from all_<mode>_stocks.txt
    if args.symbols and len(args.symbols) == 1 and args.symbols[0].lower() == "auto":
        auto_file = f"all_{args.mode}_stocks.txt"
        try:
            with open(auto_file, "r", encoding="utf-8") as f:
                text = f.read()
        except FileNotFoundError:
            print(
                f"ERROR: Auto symbols file not found: {auto_file}",
                file=sys.stderr,
            )
            return
        except Exception as e:
            print(
                f"ERROR: Failed to read auto symbols file {auto_file}: {e}",
                file=sys.stderr,
            )
            return
        input_symbols = text.split()
        if not input_symbols:
            print(
                f"ERROR: Auto symbols file {auto_file} contains no symbols.",
                file=sys.stderr,
            )
            return
    else:
        # For index mode with no symbols, use default set
        if args.mode == "id" and not args.symbols:
            input_symbols = ["^STI", "^DJI", "^IXIC", "^GSPC"]
        else:
            input_symbols = args.symbols

    exclude_symbols = args.exclude if args.exclude else []

    if args.mode == "sg":
        exclude_normalized = {ensure_si(s) for s in exclude_symbols}
        normalized_symbols = [ensure_si(s) for s in input_symbols]
    elif args.mode == "cc":
        exclude_normalized = {ensure_cc(s) for s in exclude_symbols}
        normalized_symbols = [ensure_cc(s) for s in input_symbols]
    elif args.mode == "id":
        exclude_normalized = {ensure_idx(s) for s in exclude_symbols}
        normalized_symbols = [ensure_idx(s) for s in input_symbols]
    else:  # 'us'
        exclude_normalized = {s.strip().upper() for s in exclude_symbols}
        normalized_symbols = [s.strip().upper() for s in input_symbols]

    counts = Counter(normalized_symbols)
    duplicates = [f"{sym} (x{counts[sym]})" for sym in counts if counts[sym] > 1]
    if duplicates:
        print(
            "[WARN] Duplicate codes detected (will be de-duplicated): "
            + ", ".join(duplicates)
        )

    symbols_si = [
        sym for sym in dict.fromkeys(normalized_symbols) if sym not in exclude_normalized
    ]

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols_si[0])
    except Exception:
        pass

    name_map = get_name_map(symbols_si)

    results = []

    for sym in tqdm(symbols_si, desc="Scanning", unit="symbol"):
        try:
            chart = fetch_chart_1y(sym)
            highs = chart["high"]
            lows = chart["low"]
            closes = chart["close"]

            closes_valid = [c for c in closes if c is not None]
            if len(closes_valid) == 0:
                raise ValueError("No close prices in 1Y history")

            ma5 = ma_last(closes_valid, 5)
            ma20 = ma_last(closes_valid, 20)
            ma50 = ma_last(closes_valid, 50)
            ma100 = ma_last(closes_valid, 100)
            ma200 = ma_last(closes_valid, 200)

            latest = latest_non_none(closes)

            # ATRs using the refactored generic function
            atr5 = compute_atr(highs, lows, closes, 5)
            atr20 = compute_atr(highs, lows, closes, 20)
            atr50 = compute_atr(highs, lows, closes, 50)
            atr100 = compute_atr(highs, lows, closes, 100)
            atr200 = compute_atr(highs, lows, closes, 200)

            if is_finite(ma20) and ma20 != 0:
                delta_pct = 100.0 * (latest - ma20) / ma20
            else:
                delta_pct = float("nan")

            # Z-ATR = (LC - MA20) / ATR20
            z_atr = (
                (latest - ma20) / atr20
                if (
                    is_finite(latest)
                    and is_finite(ma20)
                    and is_finite(atr20)
                    and atr20 != 0
                )
                else float("nan")
            )

            if is_finite(atr20) and is_finite(latest) and latest != 0:
                atr_lc_pct = 100.0 * atr20 / latest
            else:
                atr_lc_pct = float("nan")

            # Display symbol stripping suffixes/prefixes based on mode
            raw_code = sym
            if args.mode == "sg":
                disp_code = raw_code.removesuffix(".SI")
            elif args.mode == "cc":
                disp_code = raw_code.removesuffix("-USD")
            elif args.mode == "id":
                disp_code = raw_code[1:] if raw_code.startswith("^") else raw_code
            else:
                disp_code = raw_code

            results.append(
                {
                    "Symbol": disp_code,
                    "Name": name_map.get(sym, sym),
                    "LC": latest,
                    "MA5": ma5,
                    "MA20": ma20,
                    "MA50": ma50,
                    "MA100": ma100,
                    "MA200": ma200,
                    "Delta%": delta_pct,
                    "ATR5": atr5,
                    "ATR20": atr20,
                    "ATR50": atr50,
                    "ATR100": atr100,
                    "ATR200": atr200,
                    "Z-ATR": z_atr,
                    "ATR-LC%": atr_lc_pct,
                }
            )
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        finally:
            time.sleep(args.sleep)

    # Base set: drop rows where Delta% isn't computable (needs MA20)
    filtered = [r for r in results if is_finite(r.get("Delta%"))]

    applied = []

    # Regime filter: LC > MA200
    if args.reg_filter:
        filtered = [
            r
            for r in filtered
            if is_finite(r.get("LC")) and is_finite(r.get("MA200")) and r["LC"] > r["MA200"]
        ]
        applied.append("LC > MA200")

    # Apply optional filters
    if args.delta_thres is not None:
        # 'z' mode: keep rows where Delta% ≤ that record's Z-ATR (per-record)
        if isinstance(args.delta_thres, str) and args.delta_thres.lower() == "z":
            filtered = [
                r
                for r in filtered
                if is_finite(r.get("Z-ATR")) and r["Delta%"] <= r["Z-ATR"]
            ]
            applied.append("Delta% ≤ Z-ATR (per-record)")
        else:
            thr = float(args.delta_thres)
            if thr <= 0:
                filtered = [
                    r
                    for r in filtered
                    if is_finite(r.get("Delta%")) and r["Delta%"] <= thr
                ]
                applied.append(f"Delta% ≤ {thr:.2f}%")
            else:
                filtered = [
                    r
                    for r in filtered
                    if is_finite(r.get("Delta%")) and r["Delta%"] > thr
                ]
                applied.append(f"Delta% > {thr:.2f}%")
    if args.z_thres is not None:
        zt = float(args.z_thres)
        if zt <= 0:
            filtered = [
                r
                for r in filtered
                if is_finite(r.get("Z-ATR")) and r["Z-ATR"] <= zt
            ]
            applied.append(f"Z-ATR ≤ {zt:.2f}")
        else:
            filtered = [
                r
                for r in filtered
                if is_finite(r.get("Z-ATR")) and r["Z-ATR"] > zt
            ]
            applied.append(f"Z-ATR > {zt:.2f}")
    if args.atr_thres is not None:
        vt = float(args.atr_thres)

        def keep_volt(r):
            v = r.get("ATR-LC%", float("nan"))
            return is_finite(v) and (v >= vt)

        filtered = [r for r in filtered if keep_volt(r)]
        applied.append(f"ATR-LC% ≥ {vt:.2f}%")

    # ----- Sorting -----
    sort_by = args.sort_by
    descending = False
    metric_key = "Delta%"  # default

    if sort_by == "delta":
        metric_key = "Delta%"
        # direction based on delta_thres (numeric only)
        if args.delta_thres is not None and not (
            isinstance(args.delta_thres, str) and args.delta_thres.lower() == "z"
        ):
            thr = float(args.delta_thres)
            if thr > 0:
                descending = True  # most positive first
            else:
                descending = False  # most negative first
        else:
            # no numeric threshold -> increasing (most negative first)
            descending = False
    elif sort_by == "z":
        metric_key = "Z-ATR"
        if args.z_thres is not None:
            zt = float(args.z_thres)
            if zt > 0:
                descending = True  # most positive Z-ATR first
            else:
                descending = False  # most negative Z-ATR first
        else:
            descending = False
    elif sort_by == "atr":
        metric_key = "ATR-LC%"
        descending = True  # always largest ATR-LC% first

    if not descending:
        def sort_key(r):
            v = r.get(metric_key)
            return (0, v) if is_finite(v) else (1, float("inf"))
    else:
        def sort_key(r):
            v = r.get(metric_key)
            return (0, -v) if is_finite(v) else (1, float("inf"))

    filtered.sort(key=sort_key)

    # ==== Summary line ====
    applied_str = "; ".join(applied) if applied else "no extra filters"
    print(
        f"\nProcessed {len(results)} valid symbols, {len(filtered)} passed filter"
        f"{'s' if len(applied) > 1 else ''}: {applied_str}\n"
    )

    # ===== One-row compact table (short labels & widths) =====
    header = (
        f"{'Code':<4} {'Name':<10} "
        f"{'LC':>6} {'MA5':>6} {'MA20':>6} {'MA50':>6} {'MA100':>6} {'MA200':>6} {'ΔLC%':>6} "
        f"{'ATR5':>6} {'ATR20':>6} {'ATR200':>6} {'Z-ATR':>5} {'ATR%':>5}"
    )
    print(header)
    print("-" * len(header))

    for r in filtered:
        print(
            f"{(r['Symbol'] or '')[:4]:<4} "
            f"{(r['Name'] or '')[:10]:<10} "
            f"{fmt_price(r['LC'],      6)} "
            f"{fmt_price(r['MA5'],     6)} "
            f"{fmt_price(r['MA20'],    6)} "
            f"{fmt_price(r['MA50'],    6)} "
            f"{fmt_price(r['MA100'],   6)} "
            f"{fmt_price(r['MA200'],   6)} "
            f"{fmtf(r['Delta%'],       6, 2)} "
            f"{fmt_price(r['ATR5'],    6)} "
            f"{fmt_price(r['ATR20'],   6)} "
            f"{fmt_price(r['ATR200'],  6)} "
            f"{fmtf(r['Z-ATR'],        5, 2)} "
            f"{fmtf(r['ATR-LC%'],      5, 2)}"
        )
        stack = ma_stack_str(r)
        if stack:
            approx = ""
            lc = r.get("LC")
            ma5 = r.get("MA5")
            atr5 = r.get("ATR5")
            if is_finite(lc) and is_finite(ma5) and is_finite(atr5) and atr5 > 0:
                if abs(lc - ma5) <= 0.5 * atr5:
                    approx = " (LC ≈ MA5)"
            print(stack + approx)


if __name__ == "__main__":
    main()
