"""
scan_price_backtest.py

Mean-reversion backtester: for each symbol, pulls daily OHLCV from Yahoo Finance
(lookback window configurable via --window, default 1 year) and simulates how the
stock has historically behaved after touching a reference price level.

For each OHLC touch of start_price one episode is generated:
  Trigger  : day where start_price falls within the day's OHLC (low <= sp <= high).
  Entry    : that same day (entry_close = close).
  TP       : start_price * (1 + tp_level/100).  Detected via intraday high from the
             FOLLOWING session onward.
  WIN      : intraday high >= tp_price within max_hold trading days.
  FAIL     : max_hold trading days elapsed without TP hit.
             Eventual TP hit (if any) is recorded for reference; next episode search
             resumes from entry_i + max_hold.
  OPEN     : end of data reached before max_hold elapsed and TP not yet hit.

Usage example:
  python scan_price_backtest.py --mode sg --symbols D05 C6L --tp_level 8
  python scan_price_backtest.py --mode us --symbols AAPL MSFT NVDA --sort_by succ_pct
  python scan_price_backtest.py --mode us --symbols NVDA --price 800 --tp_level 15
  python scan_price_backtest.py --mode us --symbols TSLA NVDA MSFT --price 360 190 400
  python scan_price_backtest.py --mode us --symbols NVDA --price 190 200 210
  python scan_price_backtest.py --mode cc --symbols BTC ETH --tp_level 20
  python scan_price_backtest.py --mode sg --symbols auto --min_episodes 3 --sort_by succ_pct
  python scan_price_backtest.py --mode us --symbols AAPL --window 3

Notes:
- --mode selects:
    'sg' for SGX (codes like 'D05', 'C6L'; mapped to Yahoo by appending '.SI'),
    'us' for US stocks (codes like 'AAPL', 'GOOG'; used as-is),
    'cc' for cryptocurrencies (codes like 'BTC', 'ETH'; mapped to Yahoo by appending '-USD'),
    'id' for indexes (codes like '^STI', '^DJI'; used as-is for Yahoo, but '^' stripped in
         display; any dot-suffix tickers e.g. ES3.SI are also accepted and suffix is stripped).
- --symbols takes space-separated codes (no quotes), or 'auto' to load from all_<mode>_stocks.txt.
    When explicit symbols are provided (not 'auto'), all output filters are disabled
    automatically (equivalent to --no_filters) so every symbol is always shown.
- --price sets the reference price used for entry detection and TP calculation:
    * omit to use each symbol's own latest close (default).
    * if provided, must supply exactly N values in the same order as --symbols.
      Example: --symbols TSLA NVDA MSFT --price 360 190 400
    * an episode starts on the earliest day where start_price falls within that day's OHLC.
- --tp_level sets the take-profit distance as a percentage of start_price:
    * TP is triggered when intraday high >= start_price * (1 + tp_level/100).
    * default: 10 (10%%).
- --sort_by controls sorting of the final summary table (applies to --symbols auto only):
    * 'succ_pct':  sort by MR success rate %% (successes / total episodes), descending (default).
    * 'succ_abs':  sort by absolute number of successes, descending.
    * 'none':      keep scan order as processed.
- --min_episodes filters the output to only show symbols with at least N total episodes
    (default: 2; ignored when explicit symbols given).
- --success_thres filters the output to only show symbols whose closed-episode win rate
    (wins / (wins + fails); OPEN episodes excluded as inconclusive) meets the threshold
    (default: 0.5 = 50%%; ignored when explicit symbols given).
- --top_N keep only the top N symbols after all filters (default: 10; 0 = show all;
    ignored when explicit symbols given).
- --no_filters disables min_episodes, success_thres, and top_N filters (useful with 'auto').
- --max_hold sets the maximum holding period in trading days:
    * TP not hit within max_hold trading days → FAIL (eventual TP hit, if any, is tracked).
    * End of data reached within max_hold without TP → OPEN.
    * default: 20.
- --exclude removes the specified symbols from being processed (mode normalization is applied).
- --sleep sets the delay in seconds between Yahoo Finance requests (default: 0.5).
- --window sets the historical lookback period in years (any positive integer, default: 1).
"""

from __future__ import annotations

import argparse
import gzip
import http.cookiejar as cookielib
import json
import math
import re
import sys
import time
import urllib.request
import zlib
from collections import Counter
from datetime import date, datetime, timezone

from tqdm import tqdm

# ─── Yahoo Finance endpoints ──────────────────────────────────────────────────

YF_HOME       = "https://finance.yahoo.com/"
YF_GET_CRUMB  = "https://query1.finance.yahoo.com/v1/test/getcrumb"
YF_QUOTE_PAGE = "https://finance.yahoo.com/quote/{symbol}?p={symbol}"
YF_QUOTE_URL  = (
    "https://query1.finance.yahoo.com/v7/finance/quote"
    "?symbols={symbols}&lang=en-US&region=US"
)
YF_SEARCH_URL = (
    "https://query2.finance.yahoo.com/v1/finance/search?q={symbol}&quotesCount=1"
)
YF_CHART_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    "?interval=1d&range={range}&includeAdjustedClose=true"
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_CJ     = cookielib.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_CJ))
_CRUMB  = None


# ─── HTTP helpers (same pattern as scan_mr_ma20.py) ──────────────────────

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
            "User-Agent":      UA,
            "Accept":          "application/json,text/plain,*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.8",
            "Connection":      "keep-alive",
            "Referer":         "https://finance.yahoo.com/",
            "Origin":          "https://finance.yahoo.com",
            "Pragma":          "no-cache",
            "Cache-Control":   "no-cache",
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
            "User-Agent":      UA,
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.8",
            "Connection":      "keep-alive",
            "Referer":         "https://finance.yahoo.com/",
            "Origin":          "https://finance.yahoo.com",
            "Pragma":          "no-cache",
            "Cache-Control":   "no-cache",
        },
    )
    with _OPENER.open(req, timeout=timeout) as resp:
        data = resp.read()
        return _decompress_and_decode(resp, data)


def warm_up_cookies_and_crumb(symbol_for_visit: str):
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


# ─── Ticker normalizers ───────────────────────────────────────────────────────

def ensure_si(ticker: str) -> str:
    t = ticker.strip().upper()
    return t if t.endswith(".SI") else f"{t}.SI"


def ensure_cc(ticker: str) -> str:
    t = ticker.strip().upper()
    return t if t.endswith("-USD") else f"{t}-USD"


def ensure_idx(ticker: str) -> str:
    t = ticker.strip().upper()
    if t.startswith("^"):
        return t
    if re.search(r"\.[A-Z0-9]+$", t):
        return t
    return t


# ─── Name lookup ─────────────────────────────────────────────────────────────

def try_quote_names(symbols: list[str]) -> dict:
    name_map = {s: s for s in symbols}
    try:
        payload = http_get_json(YF_QUOTE_URL.format(symbols=",".join(symbols)))
        for q in payload.get("quoteResponse", {}).get("result", []):
            sym = q.get("symbol", "")
            nm  = (
                q.get("shortName")
                or q.get("longName")
                or q.get("displayName")
                or sym
            )
            name_map[sym] = nm
    except Exception:
        pass
    return name_map


def try_search_name(symbol: str) -> str:
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


def get_name_map(symbols: list[str]) -> dict:
    nm = try_quote_names(symbols)
    for s in symbols:
        if not nm.get(s) or nm.get(s) == s:
            nm[s] = try_search_name(s)
    return nm


# ─── Chart fetch ──────────────────────────────────────────────────────────────

def fetch_chart(symbol: str, window: str = "1y") -> dict:
    """Fetch daily OHLCV + Unix timestamps from Yahoo Finance for the given window."""
    payload = http_get_json(YF_CHART_URL.format(symbol=symbol, range=window))
    result  = payload.get("chart", {}).get("result", []) or []
    if not result:
        raise ValueError("No chart result returned")
    r0    = result[0]
    ind   = r0.get("indicators", {}) or {}
    quote = (ind.get("quote", [{}]) or [{}])[0]
    return {
        "timestamps": r0.get("timestamp") or [],
        "open":       quote.get("open")   or [],
        "high":       quote.get("high")   or [],
        "low":        quote.get("low")    or [],
        "close":      quote.get("close")  or [],
        "volume":     quote.get("volume") or [],
    }


# ─── Utilities ────────────────────────────────────────────────────────────────

def is_finite(x) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


def ts_to_date(ts) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _auto_dp(price) -> int:
    """Decimal places based on price magnitude."""
    if not is_finite(price) or price == 0:
        return 2
    if price >= 100:
        return 2
    if price >= 10:
        return 3
    if price >= 1:
        return 4
    return 5


# ─── Core analysis ────────────────────────────────────────────────────────────

def analyze_mr(
    symbol: str,
    name: str,
    chart: dict,
    start_price: float | None,
    tp_level: float,
    max_hold: int,
) -> dict:
    """
    Episode-based MR analysis using intraday high for TP detection.

    Episode model:
      Trigger  : day where start_price falls within the OHLC range (low <= sp <= high).
      Entry    : that same day (entry_close = close).
      TP       : sp * (1 + tp_level).  Checked via intraday high from the FOLLOWING session.
      WIN      : intraday high >= tp_price within max_hold trading days.
      FAIL     : max_hold trading days elapsed without TP hit.
                 Scan continues beyond max_hold to record eventual TP date if hit later.
                 Next episode search resumes from entry_i + max_hold.
      OPEN     : end of data reached before max_hold elapsed and TP not yet hit.
    """
    closes     = chart["close"]
    highs      = chart["high"]
    lows       = chart["low"]
    timestamps = chart["timestamps"]

    n = min(len(closes), len(highs), len(lows), len(timestamps))

    valid_days = [
        (i, ts, c, h, lo)
        for i, (ts, c, h, lo) in enumerate(
            zip(timestamps[:n], closes[:n], highs[:n], lows[:n])
        )
        if (
            c  is not None and is_finite(c)
            and h  is not None and is_finite(h)
            and lo is not None and is_finite(lo)
        )
    ]

    if not valid_days:
        raise ValueError("No valid OHLC prices in history")

    sp = (
        start_price
        if (start_price is not None and is_finite(start_price))
        else valid_days[-1][2]
    )

    tp_price = sp * (1 + tp_level)
    m = len(valid_days)

    episodes = []
    i = 0  # scan the full lookback window for OHLC touches of sp

    while i < m:
        # Advance to next day where sp falls within the day's OHLC
        while i < m and not (valid_days[i][4] <= sp <= valid_days[i][3]):
            i += 1
        if i >= m:
            break

        entry_i  = i
        entry_ts = valid_days[i][1]
        entry_c  = valid_days[i][2]

        # Scan the next max_hold bars (from the following session) for a TP hit
        search_end = min(entry_i + 1 + max_hold, m)
        first_tp_k = None

        for k in range(entry_i + 1, search_end):
            _, _, _, kh, _ = valid_days[k]
            if kh >= tp_price:
                first_tp_k = k
                break

        if first_tp_k is not None:
            # WIN
            outcome          = "win"
            tp_dur           = first_tp_k - entry_i
            fail_date        = None
            eventual_tp_date = None
            eventual_tp_dur  = None
            scan_end_low     = first_tp_k + 1
            i_next           = first_tp_k + 1
        elif entry_i + 1 + max_hold > m:
            # OPEN: ran out of data before max_hold expired
            outcome          = "open"
            tp_dur           = None
            fail_date        = None
            eventual_tp_date = None
            eventual_tp_dur  = None
            scan_end_low     = m
            i_next           = m
        else:
            # FAIL: max_hold exhausted — scan further for eventual TP
            outcome      = "fail"
            tp_dur       = None
            fail_date    = ts_to_date(valid_days[search_end - 1][1])
            scan_end_low = search_end
            i_next       = search_end

            eventual_tp_k = None
            for _k in range(search_end, m):
                if valid_days[_k][3] >= tp_price:
                    eventual_tp_k = _k
                    break
            eventual_tp_date = (
                ts_to_date(valid_days[eventual_tp_k][1]) if eventual_tp_k is not None else None
            )
            eventual_tp_dur = (
                eventual_tp_k - entry_i if eventual_tp_k is not None else None
            )

        scan_lows  = [valid_days[k][4] for k in range(entry_i + 1, scan_end_low)]
        min_low    = min(scan_lows) if scan_lows else sp
        min_low_ts = next(
            (valid_days[k][1] for k in range(entry_i + 1, scan_end_low)
             if valid_days[k][4] == min_low),
            None,
        ) if scan_lows else None

        episodes.append({
            "entry_date":         ts_to_date(entry_ts),
            "entry_close":        entry_c,
            "outcome":            outcome,
            "first_tp_date":      ts_to_date(valid_days[first_tp_k][1]) if first_tp_k is not None else None,
            "tp_dur_td":          tp_dur,
            "fail_date":          fail_date,
            "eventual_tp_date":   eventual_tp_date,
            "eventual_tp_dur_td": eventual_tp_dur,
            "td_elapsed":         m - entry_i,
            "min_low":            min_low,
            "min_low_date":       ts_to_date(min_low_ts) if min_low_ts else None,
        })

        if outcome == "open":
            break
        i = i_next

    successes = [ep for ep in episodes if ep["outcome"] == "win"]

    last_ep = episodes[-1] if episodes else None
    pending = last_ep if (last_ep and last_ep["outcome"] == "open") else None

    return {
        "symbol":       symbol,
        "name":         name,
        "start_price":  sp,
        "tp_price":     tp_price,
        "n_episodes":   len(episodes),
        "data_start":   ts_to_date(valid_days[0][1]),
        "data_end":     ts_to_date(valid_days[-1][1]),
        "latest_close": valid_days[-1][2],
        "episodes":     episodes,
        "successes":    successes,
        "pending":      pending,
    }


# ─── Terminal output ──────────────────────────────────────────────────────────

def _date_range_str(start_str: str, end_str: str) -> str:
    """Return 'X years Y months' duration between two YYYY-MM-DD strings."""
    start  = date.fromisoformat(start_str)
    end    = date.fromisoformat(end_str)
    months = (end.year - start.year) * 12 + (end.month - start.month)
    years, rem_m = divmod(months, 12)
    y = f"{years} year{'s' if years != 1 else ''}"
    m = f"{rem_m} month{'s' if rem_m != 1 else ''}"
    if years > 0 and rem_m > 0:
        return f"{y} {m}"
    return y if years > 0 else m


def _print_summary(results: list[dict], min_episodes: int, max_hold: int, success_thres: float, top_n: int):
    total_processed = len(results)

    def _eff_rate(r: dict) -> float:
        n_win    = len(r["successes"])
        n_fail   = sum(1 for ep in r["episodes"] if ep["outcome"] == "fail")
        n_closed = n_win + n_fail
        return n_win / n_closed if n_closed else 0.0

    filtered = [
        r for r in results
        if r["n_episodes"] >= min_episodes and _eff_rate(r) >= success_thres
    ]

    if top_n > 0 and len(filtered) > top_n:
        filtered = filtered[:top_n]

    applied_str = f"episodes >= {min_episodes}, success rate >= {success_thres:.0%}, top {top_n}"
    print(
        f"\nProcessed {total_processed} valid symbols, {len(filtered)} passed filter: "
        f"{applied_str}\n"
    )

    if not filtered:
        return

    sep = "─" * 72

    for res in filtered:
        sp = res["start_price"]
        dp = _auto_dp(sp)

        def p(x, _dp=dp) -> str:
            return f"{x:.{_dp}f}" if is_finite(x) else "N/A"

        code      = res.get("disp_code", res["symbol"])
        name      = res["name"]

        n_succ    = len(res["successes"])
        n_fail    = sum(1 for ep in res["episodes"] if ep["outcome"] == "fail")
        n_closed  = n_succ + n_fail
        pct       = n_succ / n_closed * 100 if n_closed else 0
        hist_str  = _date_range_str(res["data_start"], res["data_end"])
        succ_str  = f"{n_succ}/{n_closed} ({pct:.0f}%)"
        lc_val    = p(res["latest_close"])

        print(sep)
        print(f"  {code}  ·  {name}")
        print(sep)
        print(f"  History: {hist_str} | LC: {lc_val} | Successes: {succ_str}")
        print()

        # All episodes, most recent first
        show_eps = sorted(res["episodes"], key=lambda ep: ep["entry_date"], reverse=True)

        if not show_eps:
            print("  (no episodes)")
            print()
            continue

        # Pre-compute all row values to determine column widths
        sp_val = p(sp)
        tp_val = p(res["tp_price"])
        rows = []
        for ep in show_eps:
            outcome = ep["outcome"]
            if outcome == "win":
                exit_str = ep["first_tp_date"] or "?"
                dur      = f"{ep['tp_dur_td']} days" if ep["tp_dur_td"] is not None else "?"
                status   = "[WIN]"
            elif outcome == "fail":
                eventual_date = ep.get("eventual_tp_date")
                eventual_dur  = ep.get("eventual_tp_dur_td")
                if eventual_date:
                    exit_str = eventual_date
                    dur      = f"{eventual_dur} days"
                else:
                    exit_str = "open"
                    dur      = f"{ep['td_elapsed']} days"
                status = "[FAIL]"
            else:  # open
                exit_str = "open"
                dur      = f"{ep['td_elapsed']} days"
                status   = "[OPEN]"
            low_val = p(ep["min_low"])
            rows.append((ep["entry_date"], exit_str, dur, low_val, status))

        dur_w  = max(max(len(r[2]) for r in rows), len("Duration"))
        ep_w   = max(len(sp_val), len("EP"))
        tp_w   = max(len(tp_val), len("TP"))
        low_w  = max(max(len(r[3]) for r in rows), len("Low"))
        stat_w = max(max(len(r[4]) for r in rows), len("Status"))

        hdr = (
            f"  {'#':>3}  {'Entry':10}  {'→ Exit':12}  {'Duration':>{dur_w}}  "
            f"{'EP':>{ep_w}}  {'TP':>{tp_w}}  {'Low':>{low_w}}  Status"
        )
        rule = (
            f"  {'─'*3}  {'─'*10}  {'─'*12}  {'─'*dur_w}  "
            f"{'─'*ep_w}  {'─'*tp_w}  {'─'*low_w}  {'─'*stat_w}"
        )
        print(hdr)
        print(rule)

        for k, (entry_date, exit_str, dur, low_val, status) in enumerate(rows, 1):
            print(
                f"  {k:>3}  {entry_date:10}  → {exit_str:<10}  {dur:>{dur_w}}  "
                f"{sp_val:>{ep_w}}  {tp_val:>{tp_w}}  {low_val:>{low_w}}  {status}"
            )

        print()

    print(sep)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description=(
            "Mean-reversion backtester: uses Yahoo Finance daily data (lookback window "
            "configurable via --window, default 1 year) to show how reliably a stock bounced "
            "from a given price level to a TP target, and how far price dipped during each "
            "episode before recovering to the TP target."
        )
    )
    ap.add_argument(
        "--mode",
        choices=["sg", "us", "cc", "id"],
        required=True,
        help=(
            "'sg' SGX tickers (appends .SI),  'us' US stocks,  "
            "'cc' crypto (appends -USD),  'id' indexes"
        ),
    )
    ap.add_argument(
        "--symbols",
        nargs="+",
        help=(
            "Space-separated stock/crypto/index codes, or 'auto' to load from "
            "all_<mode>_stocks.txt."
        ),
    )
    ap.add_argument(
        "--price",
        nargs="+",
        type=float,
        default=None,
        help=(
            "Reference entry price(s).  Either omit (each symbol uses its own latest close) "
            "or provide exactly N values matching the N symbols in --symbols order.  "
            "Example: --symbols TSLA NVDA --price 360 190"
        ),
    )
    ap.add_argument(
        "--tp_level",
        type=float,
        default=10.0,
        help="Take-profit distance as a percentage of start_price (default: 10 = 10%%).",
    )
    ap.add_argument(
        "--sort_by",
        choices=["succ_pct", "succ_abs", "none"],
        default="succ_pct",
        help=(
            "Sort output by: 'succ_pct' (success %% descending, default), "
            "'succ_abs' (absolute number of successes descending), "
            "or 'none' (keep scan order)."
        ),
    )
    ap.add_argument(
        "--success_thres",
        type=float,
        default=0.5,
        help=(
            "Minimum effective success rate (wins within max_hold / total episodes) "
            "to include a symbol (default: 0.5 = 50%%)."
        ),
    )
    ap.add_argument(
        "--min_episodes",
        type=int,
        default=2,
        help=(
            "Minimum total number of episodes (successes + any open episode) required "
            "to include a symbol in the output (default: 2)."
        ),
    )
    ap.add_argument(
        "--max_hold",
        type=int,
        default=20,
        help=(
            "Maximum holding period in days for a win to count as a success. "
            "Episodes where TP took > max_hold trading days are labelled FAIL; "
            "open episodes lasting > max_hold trading days are labelled FAIL "
            "(default: 20)."
        ),
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
        "--window",
        type=int,
        default=1,
        help="Lookback window in years for historical data (default: 1).",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds to sleep between requests (default: 0.5).",
    )
    ap.add_argument(
        "--top_N",
        type=int,
        default=10,
        help=(
            "After all other filters, keep only the top N symbols (default: 10). "
            "Set to 0 to disable."
        ),
    )
    ap.add_argument(
        "--no_filters",
        action="store_true",
        help=(
            "Disable all default output filters: sets min_episodes=0, "
            "success_thres=0.0, and top_N=0 so every symbol is shown."
        ),
    )
    args = ap.parse_args()

    if args.window < 1:
        ap.error("--window must be a positive integer (years).")
    args.window   = f"{args.window}y"
    args.tp_level = args.tp_level / 100.0

    is_auto = (
        args.symbols
        and len(args.symbols) == 1
        and args.symbols[0].lower() == "auto"
    )

    # When symbols are explicitly provided, disable all filters automatically.
    # max_hold is intentionally NOT overridden here — it controls WIN/FAIL labels
    # and the successes count, which should always reflect the user's threshold.
    if not is_auto or args.no_filters:
        args.min_episodes  = 0
        args.success_thres = 0.0
        args.top_N         = 0

    # Symbols must be provided for all modes (unless using 'auto' later).
    if not args.symbols:
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
        input_symbols = args.symbols

    # Special case: 1 unique symbol + multiple start prices → expand symbol list
    _is_multi_price = (
        args.price is not None
        and len(args.price) > 1
        and len({s.lower() for s in input_symbols}) == 1
    )
    if _is_multi_price:
        input_symbols = [input_symbols[0]] * len(args.price)

    # Validate --price count against input symbols before normalization
    if args.price is not None and len(args.price) != len(input_symbols):
        print(
            f"ERROR: --price has {len(args.price)} value(s) "
            f"but --symbols resolved to {len(input_symbols)} ticker(s).  "
            "They must match 1-to-1, or omit --price entirely to use latest closes.",
            file=sys.stderr,
        )
        return

    input_start_prices: list[float | None] = (
        list(args.price) if args.price is not None
        else [None] * len(input_symbols)
    )

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
    if duplicates and not _is_multi_price:
        print(
            "[WARN] Duplicate codes detected (will be de-duplicated): "
            + ", ".join(duplicates)
        )

    # Deduplicate and exclude, carrying start_price values along
    # Key by (sym, sp) so the same symbol at different price levels is kept distinct.
    seen: set = set()
    symbols_si: list[str] = []
    start_prices_si: list[float | None] = []
    for sym, sp in zip(normalized_symbols, input_start_prices):
        key = (sym, sp)
        if key not in seen and sym not in exclude_normalized:
            seen.add(key)
            symbols_si.append(sym)
            start_prices_si.append(sp)

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols_si[0])
    except Exception:
        pass

    name_map = get_name_map(symbols_si)

    chart_cache: dict[str, dict] = {}
    sym_counts = Counter(symbols_si)
    results = []
    for sym, sp in tqdm(
        zip(symbols_si, start_prices_si),
        desc="Scanning",
        unit="symbol",
        total=len(symbols_si),
    ):
        try:
            if sym not in chart_cache:
                chart_cache[sym] = fetch_chart(sym, args.window)
                time.sleep(args.sleep)
            chart = chart_cache[sym]
            res   = analyze_mr(sym, name_map.get(sym, sym), chart, sp, args.tp_level, args.max_hold)

            # Compute display code (strip mode suffix, mirrors scan_mr_ma20.py)
            if args.mode == "sg":
                disp_code = sym.removesuffix(".SI")
            elif args.mode == "cc":
                disp_code = sym.removesuffix("-USD")
            elif args.mode == "id":
                if sym.startswith("^"):
                    disp_code = sym[1:]
                elif "." in sym:
                    disp_code = sym.rsplit(".", 1)[0]
                else:
                    disp_code = sym
            else:
                disp_code = sym
            # Append @<price> when the same symbol appears at multiple price levels
            if sym_counts[sym] > 1:
                raw_sp = res["start_price"]
                sp_str = f"{raw_sp:.0f}" if raw_sp == int(raw_sp) else f"{raw_sp:g}"
                disp_code = f"{disp_code} @ {sp_str}"
            res["disp_code"] = disp_code

            results.append(res)
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)

    # Sort before printing
    if args.sort_by in ("succ_pct", "succ_abs"):
        def _avg_win_dur(r):
            """Average tp_dur_td of WIN episodes; inf if none (sorts last)."""
            durs = [ep["tp_dur_td"] for ep in r["successes"] if ep.get("tp_dur_td") is not None]
            return sum(durs) / len(durs) if durs else float("inf")

        if args.sort_by == "succ_pct":
            results.sort(
                key=lambda r: (
                    len(r["successes"]) / r["n_episodes"] if r["n_episodes"] else 0.0,
                    -_avg_win_dur(r),
                ),
                reverse=True,
            )
        else:  # succ_abs
            results.sort(
                key=lambda r: (len(r["successes"]), -_avg_win_dur(r)),
                reverse=True,
            )

    _print_summary(results, args.min_episodes, args.max_hold, args.success_thres, args.top_N)


if __name__ == "__main__":
    main()
