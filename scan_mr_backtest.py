"""
scan_mr_backtest.py

Mean-reversion backtester driven by a Z-score threshold.  For each symbol, pulls daily
OHLCV from Yahoo Finance (lookback window configurable via --window, default 1 year) and
simulates how the stock has historically behaved after the close Z-score falls to or below
a threshold (default −2.0, equivalent to touching the BB(20,2) lower band).

Episode model:
  Trigger  : close Z-score (= (close − MA20) / SD20) <= z_thres on day T.
             The first such close in the lookback window starts episode 1.
             While an episode is active, further touches are ignored; the next
             episode search resumes only after the current episode resolves.
  Entry    : the FOLLOWING trading day (T+1).  entry_price = open on day T+1.
  TP       : entry_price * (1 + tp_level/100).  Checked via intraday high from day T+1
             onward (including the entry day itself).
  WIN      : intraday high >= tp_price within max_hold trading days.
  FAIL     : max_hold trading days elapsed without intraday high >= tp_price.
  OPEN     : end of data reached before max_hold elapsed and TP not yet hit.

Reset rule (prevents chaining episodes inside a persistent downtrend):
  After each episode, a new episode may only start once Z >= z_thres/10 has been
  seen at least once since the trigger day.  If the reset level was reached during
  the episode, the next trigger search resumes normally from i_next.  If not (e.g.
  the stock stayed deeply oversold throughout), the scan fast-forwards until
  Z >= z_thres/10 before looking for the next trigger.
  Example: z_thres = -2.0 → reset level = -0.2.

BB(20,2):
  MA20     = simple 20-day moving average of closes.
  SD20     = 20-day sample standard deviation of closes (ddof=1).
  Lower BB = MA20 − 2 × SD20.

Usage example:
  python scan_mr_backtest.py --mode sg --symbols D05 C6L --tp_level 8
  python scan_mr_backtest.py --mode us --symbols AAPL MSFT NVDA --sort_by succ_pct
  python scan_mr_backtest.py --mode us --symbols NVDA --tp_level 15
  python scan_mr_backtest.py --mode cc --symbols BTC ETH --tp_level 20
  python scan_mr_backtest.py --mode sg --symbols auto --min_episodes 3 --sort_by succ_pct
  python scan_mr_backtest.py --mode us --symbols AAPL --window 3
  python scan_mr_backtest.py --mode us --symbols NVDA --z_thres -1.5 --delta_thres -3

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
- --tp_level sets the take-profit distance as a percentage of entry_price (default: 10 = 10%).
- --z_thres sets the Z-score trigger threshold (default: -2.0).
- --delta_thres optional additional trigger filter: episode only starts when
    ΔLC% = 100*(trigger_close − MA20)/MA20 <= delta_thres.  Disabled by default.
- --max_hold caps the maximum holding period in trading days:
    * TP not hit within max_hold trading days → FAIL.
    * End of data reached before max_hold elapsed without TP → OPEN.
    * default: 20.
- --sort_by controls sorting of the final summary table (applies to --symbols auto only):
    * 'succ_pct':  sort by win rate %% (wins / total episodes), descending (default).
                   Tiebreak: succ_abs (win count), then average win-episode holding duration (shorter first).
    * 'succ_abs':  sort by absolute number of wins, descending.
                   Tiebreak: succ_pct (win rate), then average win-episode holding duration (shorter first).
    * 'none':      keep scan order.
- --min_episodes filters the output to only show symbols with at least N total episodes
    (default: 2; ignored when explicit symbols given).
- --success_thres filters to symbols whose closed-episode win rate
    (wins / (wins + fails); OPEN episodes excluded as inconclusive) >= threshold.
    Expressed as an absolute percentage (e.g. 50 = 50%%; default: 50; ignored when explicit symbols given).
- --top_N keep only the top N symbols after all filters (default: 10; 0 = show all;
    ignored when explicit symbols given).
- --no_filters disables min_episodes, success_thres, and top_N filters (useful with 'auto').
- --exclude removes the specified symbols from being processed (mode normalisation applied).
- --sleep sets the delay in seconds between Yahoo Finance requests (default: 0.5).
- --window sets the historical lookback period in years (positive integer, default: 1).
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
from datetime import datetime, timezone

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


# ─── HTTP helpers (identical to scan_price_backtest.py) ──────────────────────

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


def _fmt_z(z) -> str:
    """Format Z-score to 2 dp (matches scan_mr_ma20.py display)."""
    return f"{z:.2f}" if is_finite(z) else "N/A"


def _fmt_pct(pct) -> str:
    """Format a percentage value to 2 dp with % suffix."""
    return f"{pct:.2f}%" if is_finite(pct) else "N/A"


# ─── BB(20,2) computation (same logic as scan_mr_ma20.py) ────────────────────

def _mean(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else float("nan")


def _std_sample(vals: list[float]) -> float:
    n = len(vals)
    if n < 2:
        return float("nan")
    m = _mean(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / (n - 1))


def compute_lower_bb(closes: list[float], period: int = 20, num_std: float = 2.0) -> list:
    """
    Return lower BB for each bar.  None for the first (period-1) bars where
    there is insufficient history.  Uses ddof=1 (sample std), matching scan_mr_ma20.py.
    """
    result = []
    for i in range(len(closes)):
        if i < period - 1:
            result.append(None)
        else:
            window = closes[i - period + 1 : i + 1]
            ma = _mean(window)
            sd = _std_sample(window)
            result.append(ma - num_std * sd if (is_finite(ma) and is_finite(sd)) else None)
    return result


def compute_z_arr(closes: list[float], period: int = 20) -> list:
    """
    Return Z-score (close − MA(period)) / SD(period) for each bar.
    None for the first (period-1) bars.  Uses ddof=1, matching scan_mr_ma20.py.
    """
    result = []
    for i in range(len(closes)):
        if i < period - 1:
            result.append(None)
        else:
            window = closes[i - period + 1 : i + 1]
            ma = _mean(window)
            sd = _std_sample(window)
            result.append(
                (closes[i] - ma) / sd
                if (is_finite(ma) and is_finite(sd) and sd != 0)
                else None
            )
    return result


# ─── Core analysis ────────────────────────────────────────────────────────────

def analyze_mr_bb(
    symbol: str,
    name: str,
    chart: dict,
    tp_level: float,
    max_hold: int,
    z_thres: float = -2.0,
    delta_thres: float | None = None,
) -> dict:
    """
    Episode-based MR analysis driven by Z-score threshold touches.

    Trigger  : close Z-score (= (close − MA20) / SD20) <= z_thres,
               AND (if delta_thres is set) ΔLC% = 100*(trigger_close−MA20)/MA20 <= delta_thres.
               Default z_thres=-2.0 is equivalent to touching the BB(20,2) lower band.
    Consecutive-touch rule: once an episode is active, no new episode starts until
    the current one resolves (WIN or FAIL).  After resolution the scan resumes from
    the next bar and looks for the next trigger.

    Outcomes stored per episode:
      'win'  — intraday high >= tp_price within max_hold trading days.
      'fail' — max_hold trading days elapsed without hitting tp_price.
      'open' — end of data reached inside the max_hold window with no TP hit.
    """
    closes     = chart["close"]
    highs      = chart["high"]
    lows       = chart["low"]
    opens      = chart["open"]
    timestamps = chart["timestamps"]

    n = min(len(closes), len(highs), len(lows), len(opens), len(timestamps))

    valid_days = [
        (i, ts, c, h, lo, o)
        for i, (ts, c, h, lo, o) in enumerate(
            zip(timestamps[:n], closes[:n], highs[:n], lows[:n], opens[:n])
        )
        if (
            c  is not None and is_finite(c)
            and h  is not None and is_finite(h)
            and lo is not None and is_finite(lo)
            and o is not None and is_finite(o)
        )
    ]

    if len(valid_days) < 20:
        raise ValueError(
            f"Only {len(valid_days)} valid bars — need at least 20 to compute BB(20,2)"
        )

    m         = len(valid_days)
    close_arr = [vd[2] for vd in valid_days]
    lb_arr    = compute_lower_bb(close_arr)   # list[float | None], length m
    z_arr     = compute_z_arr(close_arr)      # list[float | None], length m

    episodes: list[dict] = []
    i = 19   # first bar with valid stats (needs 20 bars for MA20/SD20)

    while i < m:
        lb  = lb_arr[i]
        z_i = z_arr[i]

        # Skip bars where Z is unavailable or above the trigger threshold
        if z_i is None or not is_finite(z_i) or z_i > z_thres:
            i += 1
            continue

        # ── Episode start ─────────────────────────────────────────────────────
        trigger_i = i
        entry_i   = trigger_i + 1   # episode starts the following bar

        # Skip if no next bar exists or its open is invalid
        if entry_i >= m:
            i += 1
            continue
        entry_price = valid_days[entry_i][5]   # index 5 = open
        if entry_price is None or not is_finite(entry_price):
            i += 1
            continue

        lower_bb_entry = lb
        tp_price       = entry_price * (1 + tp_level)

        # Z, MA20, and ΔLC% from the trigger day's close
        z_entry      = z_i if is_finite(z_i) else float("nan")
        ma20_trigger = _mean(close_arr[trigger_i - 19 : trigger_i + 1])
        lc_pct_entry = (
            100.0 * (close_arr[trigger_i] - ma20_trigger) / ma20_trigger
            if (is_finite(ma20_trigger) and ma20_trigger != 0)
            else float("nan")
        )

        # Optional ΔLC% filter: trigger day's close vs MA20 (same value in the ΔLC% column)
        if delta_thres is not None:
            if not is_finite(lc_pct_entry) or lc_pct_entry > delta_thres:
                i += 1
                continue

        # Scan max_hold bars starting from entry_i (inclusive) for an intraday TP hit
        search_end = min(entry_i + max_hold, m)
        first_tp_k = None

        for k in range(entry_i, search_end):
            if valid_days[k][3] >= tp_price:   # index 3 = high
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
        elif entry_i + max_hold > m:
            # OPEN: ran out of data before max_hold expired
            outcome          = "open"
            tp_dur           = None
            fail_date        = None
            eventual_tp_date = None
            eventual_tp_dur  = None
            scan_end_low     = m
            i_next           = m
        else:
            # FAIL: max_hold exhausted — but keep scanning to see if TP eventually hits
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

        # Min intraday low during the episode (entry_i → scan_end_low, inclusive)
        scan_lows  = [valid_days[k][4] for k in range(entry_i, scan_end_low)]
        min_low    = min(scan_lows) if scan_lows else entry_price
        min_low_ts = next(
            (valid_days[k][1] for k in range(entry_i, scan_end_low)
             if valid_days[k][4] == min_low),
            None,
        ) if scan_lows else None

        # Max intraday high during the episode (entry_i → scan_end_low, inclusive)
        scan_highs  = [valid_days[k][3] for k in range(entry_i, scan_end_low)]
        max_high    = max(scan_highs) if scan_highs else entry_price
        max_high_ts = next(
            (valid_days[k][1] for k in range(entry_i, scan_end_low)
             if valid_days[k][3] == max_high),
            None,
        ) if scan_highs else None

        episodes.append({
            "entry_date":         ts_to_date(valid_days[entry_i][1]),
            "entry_price":        entry_price,
            "lower_bb":           lower_bb_entry,
            "z":                  z_entry,
            "lc_pct":             lc_pct_entry,
            "tp_price":           tp_price,
            "outcome":            outcome,
            "first_tp_date":      ts_to_date(valid_days[first_tp_k][1]) if first_tp_k is not None else None,
            "tp_dur_td":          tp_dur,
            "fail_date":          fail_date,
            "eventual_tp_date":   eventual_tp_date,
            "eventual_tp_dur_td": eventual_tp_dur,
            "td_elapsed":         m - entry_i,   # trading bars from entry to end of data
            "min_low":            min_low,
            "min_low_date":       ts_to_date(min_low_ts) if min_low_ts else None,
            "max_high":           max_high,
            "max_high_date":      ts_to_date(max_high_ts) if max_high_ts else None,
        })

        # ── Reset condition ───────────────────────────────────────────────────
        # A new episode is only allowed after Z >= z_thres/10 has been observed
        # at least once since the trigger.  This prevents chaining episodes
        # inside a persistent downtrend.  Check whether the reset was already
        # seen within the episode window (trigger_i .. i_next-1).
        reset_level = z_thres / 10.0
        reset_met = any(
            z_arr[k] is not None and is_finite(z_arr[k]) and z_arr[k] >= reset_level
            for k in range(trigger_i, i_next)
        )

        if reset_met:
            i = i_next
        else:
            # Fast-forward until the first bar where Z >= reset_level, then
            # start looking for the next trigger from that point.
            i = i_next
            while i < m:
                z_j = z_arr[i]
                if z_j is not None and is_finite(z_j) and z_j >= reset_level:
                    break
                i += 1

    successes = [ep for ep in episodes if ep["outcome"] == "win"]
    last_ep   = episodes[-1] if episodes else None
    pending   = last_ep if (last_ep and last_ep["outcome"] == "open") else None

    # Today's Z and ΔLC% for the summary header (matches scan_mr_ma20.py output)
    today_z = (
        z_arr[-1]
        if z_arr and z_arr[-1] is not None and is_finite(z_arr[-1])
        else float("nan")
    )
    today_ma20 = _mean(close_arr[-20:]) if len(close_arr) >= 20 else float("nan")
    today_lc_pct = (
        100.0 * (close_arr[-1] - today_ma20) / today_ma20
        if is_finite(today_ma20) and today_ma20 != 0
        else float("nan")
    )

    return {
        "symbol":       symbol,
        "name":         name,
        "n_episodes":   len(episodes),
        "data_start":   ts_to_date(valid_days[0][1]),
        "data_end":     ts_to_date(valid_days[-1][1]),
        "latest_close": valid_days[-1][2],
        "today_z":      today_z,
        "today_lc_pct": today_lc_pct,
        "episodes":     episodes,
        "successes":    successes,
        "pending":      pending,
    }


# ─── Terminal output ──────────────────────────────────────────────────────────

def _print_summary(
    results: list[dict],
    min_episodes: int,
    success_thres: float,
    top_n: int,
    max_hold: int = 20,
):
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
        lc  = res["latest_close"]
        dp  = _auto_dp(lc)

        def p(x, _dp=dp) -> str:
            return f"{x:.{_dp}f}" if is_finite(x) else "N/A"

        code            = res.get("disp_code", res["symbol"])
        n_succ          = len(res["successes"])
        n_fail          = sum(1 for ep in res["episodes"] if ep["outcome"] == "fail")
        n_closed        = n_succ + n_fail
        pct             = n_succ / n_closed * 100 if n_closed else 0
        succ_str        = f"{n_succ}/{n_closed} ({pct:.0f}%)"
        z_today_str     = _fmt_z(res.get("today_z", float("nan")))
        delta_today_str = _fmt_pct(res.get("today_lc_pct", float("nan")))

        print(sep)
        print(f"  {code}  ·  {res['name']}")
        print(sep)
        print(f"  LC: {p(lc)} | ΔLC%: {delta_today_str} | Z: {z_today_str} | Successes: {succ_str}")
        print()

        # Most recent episode first
        show_eps = sorted(res["episodes"], key=lambda ep: ep["entry_date"], reverse=True)

        if not show_eps:
            print("  (no BB(20,2) lower-band touches in this window)")
            print()
            continue

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
                    # TP hit after max_hold — report actual trading days from entry
                    exit_str = eventual_date
                    dur      = f"{eventual_dur} days"
                else:
                    # TP still not hit — show trading days elapsed to end of data
                    elapsed  = ep["td_elapsed"]
                    exit_str = "open"
                    dur      = f"{elapsed} days"
                status = "[FAIL]"
            else:  # open
                elapsed  = ep["td_elapsed"]
                exit_str = "open"
                dur      = f"{elapsed} days"
                status   = "[FAIL]" if elapsed >= max_hold else "[OPEN]"

            rows.append((
                ep["entry_date"],
                exit_str,
                dur,
                _fmt_pct(ep["lc_pct"]),
                _fmt_z(ep["z"]),
                p(ep["entry_price"]),
                p(ep["tp_price"]),
                p(ep["max_high"]),
                p(ep["min_low"]),
                status,
            ))

        dur_w  = max(max(len(r[2]) for r in rows), len("Duration"))
        pct_w  = max(max(len(r[3]) for r in rows), len("ΔLC%"))
        z_w    = max(max(len(r[4]) for r in rows), len("Z"))
        ep_w   = max(max(len(r[5]) for r in rows), len("EP"))
        tp_w   = max(max(len(r[6]) for r in rows), len("TP"))
        high_w = max(max(len(r[7]) for r in rows), len("High"))
        low_w  = max(max(len(r[8]) for r in rows), len("Low"))
        stat_w = max(max(len(r[9]) for r in rows), len("Status"))

        hdr = (
            f"  {'#':>3}  {'Entry':10}  {'→ Exit':12}  {'Duration':>{dur_w}}  "
            f"{'ΔLC%':>{pct_w}}  {'Z':>{z_w}}  {'EP':>{ep_w}}  {'TP':>{tp_w}}  {'High':>{high_w}}  {'Low':>{low_w}}  Status"
        )
        rule = (
            f"  {'─'*3}  {'─'*10}  {'─'*12}  {'─'*dur_w}  "
            f"{'─'*pct_w}  {'─'*z_w}  {'─'*ep_w}  {'─'*tp_w}  {'─'*high_w}  {'─'*low_w}  {'─'*stat_w}"
        )
        print(hdr)
        print(rule)

        for k, (entry_date, exit_str, dur, pct_val, z_val, ep_val, tp_val, high_val, low_val, status) in enumerate(rows, 1):
            print(
                f"  {k:>3}  {entry_date:10}  → {exit_str:<10}  {dur:>{dur_w}}  "
                f"{pct_val:>{pct_w}}  {z_val:>{z_w}}  {ep_val:>{ep_w}}  {tp_val:>{tp_w}}  {high_val:>{high_w}}  {low_val:>{low_w}}  {status}"
            )

        print()

    print(sep)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description=(
            "Mean-reversion backtester: uses Yahoo Finance daily data to backtest "
            "how reliably a stock bounced from a BB(20,2) lower-band close touch "
            "to a TP target within a maximum holding period."
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
        "--tp_level",
        type=float,
        default=10.0,
        help="Take-profit as a percentage of entry_price (default: 10 = 10%%).",
    )
    ap.add_argument(
        "--z_thres",
        type=float,
        default=-2.0,
        help=(
            "Z-score threshold for episode trigger: episode starts when the close "
            "Z-score (= (close − MA20) / SD20) <= this value "
            "(default: -2.0, equivalent to touching the BB(20,2) lower band)."
        ),
    )
    ap.add_argument(
        "--delta_thres",
        type=float,
        default=None,
        help=(
            "ΔLC%% filter for episode trigger: episode only starts when "
            "100*(trigger_close − MA20)/MA20 <= this value in addition to the Z-score condition. "
            "Disabled by default (no filter). Negative values select deeper oversold entries "
            "(e.g. --delta_thres -5 requires trigger close to be at least 5%% below MA20)."
        ),
    )
    ap.add_argument(
        "--sort_by",
        choices=["succ_pct", "succ_abs", "none"],
        default="succ_pct",
        help=(
            "Sort output: 'succ_pct' (win %% descending; tiebreak: succ_abs then avg win duration, default), "
            "'succ_abs' (win count descending; tiebreak: succ_pct then avg win duration), or 'none'."
        ),
    )
    ap.add_argument(
        "--success_thres",
        type=float,
        default=None,
        help=(
            "Minimum closed-episode win rate (wins / (wins + fails); OPEN excluded) to include a "
            "symbol, expressed as an absolute percentage (e.g. 50 = 50%%; default: 50)."
        ),
    )
    ap.add_argument(
        "--min_episodes",
        type=int,
        default=None,
        help="Minimum total episodes to include a symbol (default: 2).",
    )
    ap.add_argument(
        "--max_hold",
        type=int,
        default=20,
        help=(
            "Maximum holding period in trading days. "
            "TP not hit within max_hold days → FAIL; "
            "end of data reached within max_hold → OPEN (default: 20)."
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
        help="Lookback window in years (default: 1).",
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
        default=None,
        help="Keep only the top N symbols after all filters (default: 10; 0 = show all).",
    )
    ap.add_argument(
        "--no_filters",
        action="store_true",
        help=(
            "Disable all default output filters: sets min_episodes=0, "
            "success_thres=0.0, and top_N=0."
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

    # When symbols are explicitly provided, disable filters that weren't explicitly set.
    if not is_auto or args.no_filters:
        if args.min_episodes  is None: args.min_episodes  = 0
        if args.success_thres is None: args.success_thres = 0.0
        if args.top_N         is None: args.top_N         = 0
    # Apply true defaults for auto mode (or when filters were explicitly passed).
    if args.min_episodes  is None: args.min_episodes  = 2
    if args.success_thres is None: args.success_thres = 50.0
    if args.top_N         is None: args.top_N         = 10
    args.success_thres /= 100.0  # convert absolute percent input to fraction

    if not args.symbols:
        print("ERROR: No symbols provided. Please supply at least one via --symbols.", file=sys.stderr)
        return

    # Resolve 'auto' from file
    if is_auto:
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
    else:  # us
        exclude_normalized = {s.strip().upper() for s in exclude_symbols}
        normalized_symbols = [s.strip().upper() for s in input_symbols]

    counts     = Counter(normalized_symbols)
    duplicates = [f"{sym} (x{counts[sym]})" for sym in counts if counts[sym] > 1]
    if duplicates:
        print("[WARN] Duplicate codes detected (will be de-duplicated): " + ", ".join(duplicates))

    seen: set      = set()
    symbols_list: list[str] = []
    for sym in normalized_symbols:
        if sym not in seen and sym not in exclude_normalized:
            seen.add(sym)
            symbols_list.append(sym)

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols_list[0])
    except Exception:
        pass

    name_map = get_name_map(symbols_list)

    results = []
    for sym in tqdm(symbols_list, desc="Scanning", unit="symbol"):
        try:
            chart = fetch_chart(sym, args.window)
            res   = analyze_mr_bb(sym, name_map.get(sym, sym), chart, args.tp_level, args.max_hold, args.z_thres, args.delta_thres)

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
            res["disp_code"] = disp_code

            results.append(res)
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        finally:
            time.sleep(args.sleep)

    if args.sort_by in ("succ_pct", "succ_abs"):
        def _avg_win_dur(r):
            """Average tp_dur_td of WIN episodes; inf if no wins (sorts last)."""
            durs = [ep["tp_dur_td"] for ep in r["successes"] if ep.get("tp_dur_td") is not None]
            return sum(durs) / len(durs) if durs else float("inf")

        if args.sort_by == "succ_pct":
            results.sort(
                key=lambda r: (
                    len(r["successes"]) / r["n_episodes"] if r["n_episodes"] else 0.0,
                    len(r["successes"]),
                    -_avg_win_dur(r),
                ),
                reverse=True,
            )
        else:  # succ_abs
            results.sort(
                key=lambda r: (
                    len(r["successes"]),
                    len(r["successes"]) / r["n_episodes"] if r["n_episodes"] else 0.0,
                    -_avg_win_dur(r),
                ),
                reverse=True,
            )

    _print_summary(results, args.min_episodes, args.success_thres, args.top_N, args.max_hold)


if __name__ == "__main__":
    main()
