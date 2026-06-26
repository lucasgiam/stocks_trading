"""
scan_technicals.py

Scan SGX or US tickers on Yahoo and answer 13 yes/no (1/0) technical
questions per symbol, then report a total score per symbol.

Data is pulled the same way as scan_mr_ma20.py (Yahoo chart endpoint,
same cookie/crumb warm-up, same HTTP plumbing), but over a 2-year window
instead of 1-year, since several questions need a 200-day SMA trend
computed 30 trading days back (requires ~230 days of history) plus a
prior 60-day comparison window before the most recent 30 days (~90 days).

Usage examples:
  python scan_technicals.py --mode sg --symbols D05 C6L --sector S58 --sort_by score
  python scan_technicals.py --mode us --symbols AAPL MSFT NVDA --sector XLK
  python scan_technicals.py --mode us --symbols AAPL MSFT --sort_by none

Notes:
- --mode selects:
    'sg' for SGX (codes like 'D05', 'C6L'; mapped to Yahoo by appending '.SI'),
         broad market index used is '^STI'.
    'us' for US stocks (codes like 'AAPL', 'GOOG'; used as-is),
         broad market index used is '^GSPC'.
- --symbols takes space-separated codes (no quotes), or 'auto' to load from
  all_<mode>_stocks.txt.
- --exclude removes the specified symbols from being processed (normalization
  by mode is applied, same as for --symbols).
- --sector (optional): a single symbol representing the relevant sector ETF
  or industry benchmark, normalized the same way as --symbols. If provided,
  all 13 questions are answered. If omitted, Q1, Q3, and Q12 (which require
  a sector reference) are skipped, leaving 10 questions.
- --sort_by:
    'score' (default): sort rows by total score, descending.
    'none': no sorting; keep scan order.
- "past 3 months" = past 50 trading days throughout.
- Volatility questions (Q12/Q13) use ATR50 expressed as a percent of price
  (ATR50 / price * 100), so that comparisons are fair across instruments
  trading at very different price levels (e.g. a stock vs. an index).
- Q11 (liquidity) is not directly observable from chart data alone (no
  bid/ask spread is available from this endpoint), so it is approximated
  using the only two data series available - close and volume - over the
  past 30 trading days:
    * average daily dollar volume (close * volume) >= $200,000, and
    * no more than 3 "illiquid days" in the past 30, where an illiquid day
      is one with zero volume or volume below 10% of the 30-day average
      volume (used as a proxy for thin/absent trading and, indirectly,
      wide bid-ask spreads).
  These thresholds are reasonable defaults, not externally specified;
  adjust LIQUIDITY_MIN_DOLLAR_VOL / LIQUIDITY_MAX_ILLIQUID_DAYS below if
  a different liquidity bar is wanted.
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
import math
from collections import Counter
from tqdm import tqdm

# Yahoo endpoints
YF_HOME = "https://finance.yahoo.com/"
YF_GET_CRUMB = "https://query1.finance.yahoo.com/v1/test/getcrumb"
YF_QUOTE_PAGE = "https://finance.yahoo.com/quote/{symbol}?p={symbol}"
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}&lang=en-US&region=US"
YF_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search?q={symbol}&quotesCount=1"

YF_CHART_2Y_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?"
    "interval=1d&range=2y"
)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
_CJ = cookielib.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_CJ))
_CRUMB = None  # filled by warm_up_cookies_and_crumb()

# ---------- Q11 liquidity thresholds (see module docstring) ----------
LIQUIDITY_MIN_DOLLAR_VOL = 200_000.0
LIQUIDITY_MAX_ILLIQUID_DAYS = 3

# ---------- short column headers (max 8 chars) for each of the 13 questions ----------
QUESTION_LABELS = {
    1: "SEC>200",   # sector ETF/benchmark above & trending up on its 200-SMA
    2: "IDX>200",   # broad index above & trending up on its 200-SMA
    3: "RS>SEC",    # stock's 3mo return beats the sector ETF/benchmark
    4: "RS>IDX",    # stock's 3mo return beats the broad index
    5: ">200&UP",   # stock above & trending up on its own 200-SMA
    6: ">50&OK",    # stock above its 50-SMA, but not >20% above it
    7: "50>200",    # stock's 50-SMA above its 200-SMA (golden-cross regime)
    8: "NEWHIGH",   # made a fresh 30d closing high vs. the prior 60d
    9: "NONEWLOW",  # no close in the past 30d broke below the prior 60d low
    10: "VOLUMEUP", # more high-volume up days than down days (30d)
    11: "LIQUID",   # adequate trading liquidity
    12: "VOL>SEC",  # ATR50% volatility higher than the sector ETF/benchmark
    13: "VOL>IDX",  # ATR50% volatility higher than the broad index
}


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


def warm_up_cookies_and_crumb(symbol_for_visit: str):
    """Make Yahoo happy; try to fetch crumb token."""
    global _CRUMB
    try:
        _ = http_get_text(YF_HOME)
        time.sleep(0.3)
        _ = http_get_text(YF_QUOTE_PAGE.format(symbol=symbol_for_visit))
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


def try_quote_names(symbols):
    """Fast path: quote endpoint for names."""
    name_map = {s: s for s in symbols}
    try:
        payload = http_get_json(YF_QUOTE_URL.format(symbols=",".join(symbols)))
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
        pass
    return name_map


def try_search_name(symbol):
    try:
        p = http_get_json(YF_SEARCH_URL.format(symbol=symbol))
        quotes = p.get("quotes", []) or []
        if quotes:
            return (
                quotes[0].get("shortname")
                or quotes[0].get("longname")
                or symbol
            )
    except Exception:
        pass
    return symbol


def get_name_map(symbols):
    nm = try_quote_names(symbols)
    for s in symbols:
        if not nm.get(s) or nm.get(s) == s:
            nm[s] = try_search_name(s)
    return nm


def fetch_chart_2y(symbol):
    """Return dict with arrays: open, high, low, close, volume (may contain None),
    plus regular_market_price from meta."""
    payload = http_get_json(YF_CHART_2Y_URL.format(symbol=symbol))
    result = payload.get("chart", {}).get("result", []) or []
    if not result:
        raise ValueError("No chart result")
    r0 = result[0]
    meta = r0.get("meta", {}) or {}
    ind = (r0.get("indicators", {}) or {})
    quote = (ind.get("quote", [{}]) or [{}])[0]
    return {
        "open": quote.get("open") or [],
        "high": quote.get("high") or [],
        "low": quote.get("low") or [],
        "close": quote.get("close") or [],
        "volume": quote.get("volume") or [],
        "regular_market_price": meta.get("regularMarketPrice"),
    }


def mean(vals):
    return sum(vals) / len(vals) if vals else float("nan")


def is_finite(x):
    return isinstance(x, (int, float)) and math.isfinite(x)


def sma(values, n, end):
    """Simple moving average over the n values ending right before index `end`."""
    if end < n:
        return float("nan")
    window = values[end - n:end]
    return mean(window)


def true_range(high, low, prev_close):
    if not (is_finite(high) and is_finite(low) and is_finite(prev_close)):
        return float("nan")
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr_last(highs, lows, closes, n):
    """ATR(n) as simple average of the last n True Range values."""
    trs = []
    m = min(len(highs), len(lows), len(closes))
    if m < 2:
        return float("nan")
    for i in range(1, m):
        tr = true_range(highs[i], lows[i], closes[i - 1])
        if is_finite(tr):
            trs.append(tr)
    if len(trs) < n:
        return float("nan")
    return mean(trs[-n:])


def build_rows(chart):
    """Chronological list of {close, high, low, volume} for days with a valid close."""
    closes = chart["close"]
    highs = chart["high"]
    lows = chart["low"]
    vols = chart["volume"]
    rows = []
    for i, c in enumerate(closes):
        if c is None:
            continue
        rows.append(
            {
                "close": c,
                "high": highs[i] if i < len(highs) else None,
                "low": lows[i] if i < len(lows) else None,
                "volume": vols[i] if i < len(vols) else None,
            }
        )
    return rows


def compute_struct(chart):
    """Derive all the per-instrument metrics needed across the 13 questions."""
    rows = build_rows(chart)
    closes = [r["close"] for r in rows]
    highs = [r["high"] if is_finite(r["high"]) else float("nan") for r in rows]
    lows = [r["low"] if is_finite(r["low"]) else float("nan") for r in rows]
    n = len(closes)

    rmp = chart.get("regular_market_price")
    price = rmp if is_finite(rmp) else (closes[-1] if closes else float("nan"))

    sma200_now = sma(closes, 200, n)
    sma200_30ago = sma(closes, 200, n - 30) if n - 30 >= 0 else float("nan")
    sma50_now = sma(closes, 50, n)

    if n >= 51 and is_finite(closes[-1]) and is_finite(closes[-51]) and closes[-51] != 0:
        ret50 = closes[-1] / closes[-51] - 1.0
    else:
        ret50 = float("nan")

    atr50 = atr_last(highs, lows, closes, 50)
    atr50_pct = 100.0 * atr50 / price if is_finite(atr50) and is_finite(price) and price != 0 else float("nan")

    return {
        "rows": rows,
        "closes": closes,
        "price": price,
        "sma200_now": sma200_now,
        "sma200_30ago": sma200_30ago,
        "sma50_now": sma50_now,
        "ret50": ret50,
        "atr50_pct": atr50_pct,
    }


def above_sma_and_trending(price, sma_now, sma_30ago):
    return (
        is_finite(price)
        and is_finite(sma_now)
        and is_finite(sma_30ago)
        and price > sma_now
        and sma_now >= sma_30ago
    )


def evaluate_questions(stock, sector, idx, has_sector):
    q = {}

    if has_sector:
        q[1] = above_sma_and_trending(sector["price"], sector["sma200_now"], sector["sma200_30ago"])
    q[2] = above_sma_and_trending(idx["price"], idx["sma200_now"], idx["sma200_30ago"])

    if has_sector:
        q[3] = is_finite(stock["ret50"]) and is_finite(sector["ret50"]) and stock["ret50"] > sector["ret50"]
    q[4] = is_finite(stock["ret50"]) and is_finite(idx["ret50"]) and stock["ret50"] > idx["ret50"]

    q[5] = above_sma_and_trending(stock["price"], stock["sma200_now"], stock["sma200_30ago"])

    q[6] = (
        is_finite(stock["price"])
        and is_finite(stock["sma50_now"])
        and stock["price"] > stock["sma50_now"]
        and stock["price"] <= stock["sma50_now"] * 1.20
    )

    q[7] = is_finite(stock["sma50_now"]) and is_finite(stock["sma200_now"]) and stock["sma50_now"] > stock["sma200_now"]

    closes = stock["closes"]
    n = len(closes)
    if n >= 90:
        prior60 = closes[n - 90:n - 30]
        last30 = closes[n - 30:n]
        q[8] = any(c > max(prior60) for c in last30)
        q[9] = not any(c < min(prior60) for c in last30)
    else:
        q[8] = False
        q[9] = False

    rows = stock["rows"]
    if n >= 31:
        idxs = range(n - 30, n)
        vols30 = [rows[i]["volume"] for i in idxs if is_finite(rows[i]["volume"])]
        avg_vol30 = mean(vols30) if vols30 else float("nan")
        up = down = 0
        for i in idxs:
            prev_c, cur_c, vol = rows[i - 1]["close"], rows[i]["close"], rows[i]["volume"]
            if not (is_finite(prev_c) and is_finite(cur_c) and is_finite(vol) and is_finite(avg_vol30)):
                continue
            if vol <= avg_vol30:
                continue
            if cur_c > prev_c:
                up += 1
            elif cur_c < prev_c:
                down += 1
        q[10] = up > down
    else:
        q[10] = False

    if n >= 30:
        last30_rows = rows[n - 30:n]
        dollar_vols = [
            r["close"] * r["volume"]
            for r in last30_rows
            if is_finite(r["close"]) and is_finite(r["volume"])
        ]
        vols_only = [r["volume"] for r in last30_rows if is_finite(r["volume"])]
        avg_dollar_vol = mean(dollar_vols) if dollar_vols else float("nan")
        avg_vol = mean(vols_only) if vols_only else float("nan")
        illiquid_days = 0
        for r in last30_rows:
            v = r["volume"]
            if not is_finite(v) or v == 0 or (is_finite(avg_vol) and avg_vol > 0 and v < 0.1 * avg_vol):
                illiquid_days += 1
        q[11] = (
            is_finite(avg_dollar_vol)
            and avg_dollar_vol >= LIQUIDITY_MIN_DOLLAR_VOL
            and illiquid_days <= LIQUIDITY_MAX_ILLIQUID_DAYS
        )
    else:
        q[11] = False

    if has_sector:
        q[12] = is_finite(stock["atr50_pct"]) and is_finite(sector["atr50_pct"]) and stock["atr50_pct"] > sector["atr50_pct"]
    q[13] = is_finite(stock["atr50_pct"]) and is_finite(idx["atr50_pct"]) and stock["atr50_pct"] > idx["atr50_pct"]

    return q


def main():
    ap = argparse.ArgumentParser(
        description="Scan SGX/US tickers (Yahoo) and score 13 technical yes/no questions per symbol."
    )
    ap.add_argument(
        "--mode",
        choices=["sg", "us"],
        required=True,
        help="Market mode: 'sg' for SGX (codes like D05, C6L; '.SI' appended; broad index ^STI), "
             "'us' for US stocks (codes like AAPL, GOOG; used as-is; broad index ^GSPC).",
    )
    ap.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="Space-separated stock codes (e.g., D05 C6L for SGX; AAPL GOOG for US), "
             "or 'auto' to load from all_<mode>_stocks.txt.",
    )
    ap.add_argument(
        "--exclude",
        nargs="+",
        help="Space-separated codes to exclude ('.SI' optional for SGX).",
    )
    ap.add_argument(
        "--sector",
        default=None,
        help="Single symbol representing the relevant sector ETF / industry benchmark. "
             "If provided, all 13 questions are answered; otherwise Q1/Q3/Q12 are skipped.",
    )
    ap.add_argument(
        "--sort_by",
        choices=["none", "score"],
        default="score",
        help="'score' (default): sort rows by total score, descending. 'none': keep scan order.",
    )
    ap.add_argument(
        "--score_thres",
        type=int,
        default=0,
        help="Minimum score required for a symbol to be shown in the output table (default 0, i.e. no filtering).",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.3,
        help="Seconds to sleep between requests.",
    )
    args = ap.parse_args()

    normalize = ensure_si if args.mode == "sg" else (lambda t: t.strip().upper())
    broad_index_symbol = "^STI" if args.mode == "sg" else "^GSPC"

    # Handle 'auto' mode for symbols: load from all_<mode>_stocks.txt
    if len(args.symbols) == 1 and args.symbols[0].lower() == "auto":
        auto_file = f"all_{args.mode}_stocks.txt"
        try:
            with open(auto_file, "r", encoding="utf-8") as f:
                text = f.read()
        except FileNotFoundError:
            print(f"ERROR: Auto symbols file not found: {auto_file}", file=sys.stderr)
            return
        except Exception as e:
            print(f"ERROR: Failed to read auto symbols file {auto_file}: {e}", file=sys.stderr)
            return
        input_symbols = text.split()
        if not input_symbols:
            print(f"ERROR: Auto symbols file {auto_file} contains no symbols.", file=sys.stderr)
            return
    else:
        input_symbols = args.symbols

    exclude_normalized = {normalize(s) for s in (args.exclude or [])}

    counts = Counter(normalize(s) for s in input_symbols)
    duplicates = [f"{sym} (x{n})" for sym, n in counts.items() if n > 1]
    if duplicates:
        print("[WARN] Duplicate codes detected (will be de-duplicated): " + ", ".join(duplicates))

    symbols = [
        sym for sym in dict.fromkeys(normalize(s) for s in input_symbols)
        if sym not in exclude_normalized
    ]
    if not symbols:
        print("ERROR: No symbols left to scan after exclusions.", file=sys.stderr)
        return

    has_sector = bool(args.sector)
    sector_symbol = normalize(args.sector) if has_sector else None

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols[0])
    except Exception:
        pass

    try:
        idx_chart = fetch_chart_2y(broad_index_symbol)
        idx_struct = compute_struct(idx_chart)
    except Exception as e:
        print(f"ERROR: Failed to fetch broad market index {broad_index_symbol}: {e}", file=sys.stderr)
        return

    sector_struct = None
    if has_sector:
        try:
            sector_chart = fetch_chart_2y(sector_symbol)
            sector_struct = compute_struct(sector_chart)
        except Exception as e:
            print(f"ERROR: Failed to fetch sector reference {sector_symbol}: {e}", file=sys.stderr)
            return

    name_map = get_name_map(symbols)

    results = []
    for sym in tqdm(symbols, desc="Scanning", unit="symbol"):
        try:
            chart = fetch_chart_2y(sym)
            stock_struct = compute_struct(chart)
            q = evaluate_questions(stock_struct, sector_struct, idx_struct, has_sector)
            score = sum(1 for v in q.values() if v)
            disp_code = sym.removesuffix(".SI") if args.mode == "sg" else sym
            results.append(
                {
                    "Symbol": disp_code,
                    "Name": name_map.get(sym, sym),
                    "Q": q,
                    "Score": score,
                    "ATR50%": stock_struct["atr50_pct"],
                }
            )
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        finally:
            time.sleep(args.sleep)

    if args.sort_by == "score":
        # Primary: Score descending. Tiebreaker: ATR50% descending (more volatile ranks higher).
        results.sort(
            key=lambda r: (r["Score"], r["ATR50%"] if is_finite(r["ATR50%"]) else float("-inf")),
            reverse=True,
        )

    all_qs = list(range(1, 14))
    qs = all_qs if has_sector else [n for n in all_qs if n not in (1, 3, 12)]
    max_score = len(qs)

    filtered = [r for r in results if r["Score"] >= args.score_thres]

    print(
        f"\nMode={args.mode} | Broad index={broad_index_symbol}"
        f"{' | Sector ref=' + sector_symbol if has_sector else ' | Sector ref=(none, Q1/Q3/Q12 skipped)'}"
    )
    print(
        f"Scored {len(results)} symbols out of {max_score} applicable questions, "
        f"{len(filtered)} passed score_thres >= {args.score_thres}.\n"
    )

    q_headers = " ".join(f"{QUESTION_LABELS[n]:>8}" for n in qs)
    header = f"{'Code':<6} {'Name':<32} {q_headers} {'Score':>6}"
    print(header)
    print("-" * len(header))

    for r in filtered:
        q_cells = " ".join(f"{1 if r['Q'][n] else 0:>8}" for n in qs)
        print(
            f"{(r['Symbol'] or '')[:6]:<6} "
            f"{(r['Name'] or '')[:32]:<32} "
            f"{q_cells} "
            f"{r['Score']:>3}/{max_score}"
        )

    if filtered:
        print()
        print("Symbol list:")
        print(" ".join(r["Symbol"] for r in filtered))
        print()


if __name__ == "__main__":
    main()
