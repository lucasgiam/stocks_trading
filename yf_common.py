"""
yf_common.py

Shared Yahoo Finance HTTP layer, ticker normalizers, name lookup, chart fetch,
and common math/formatting utilities used by all scan_*.py scripts.
"""
from __future__ import annotations

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
from pathlib import Path

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
YF_CHART_URL  = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    "?interval=1d&range={range}"
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_CJ     = cookielib.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_CJ))
_CRUMB: str | None = None


# ─── HTTP helpers ─────────────────────────────────────────────────────────────

def _decompress_and_decode(resp, data: bytes) -> str:
    enc = (resp.headers.get("Content-Encoding") or "").lower()
    if enc == "gzip" or (len(data) > 2 and data[:2] == b"\x1f\x8b"):
        data = gzip.decompress(data)
    elif enc == "deflate":
        data = zlib.decompress(data, -zlib.MAX_WBITS)
    return data.decode("utf-8", errors="replace")


def http_get_json(url: str, timeout: int = 20):
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


def http_get_text(url: str, timeout: int = 20) -> str:
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


def warm_up_cookies_and_crumb(symbol_for_visit: str) -> None:
    global _CRUMB
    try:
        http_get_text(YF_HOME)
        time.sleep(0.3)
        http_get_text(YF_QUOTE_PAGE.format(symbol=symbol_for_visit))
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
    """Fetch daily OHLCV + timestamps from Yahoo Finance for the given window."""
    payload = http_get_json(YF_CHART_URL.format(symbol=symbol, range=window))
    result  = payload.get("chart", {}).get("result", []) or []
    if not result:
        raise ValueError("No chart result returned")
    r0    = result[0]
    meta  = r0.get("meta", {}) or {}
    ind   = r0.get("indicators", {}) or {}
    quote = (ind.get("quote", [{}]) or [{}])[0]
    return {
        "timestamps":           r0.get("timestamp") or [],
        "open":                 quote.get("open")   or [],
        "high":                 quote.get("high")   or [],
        "low":                  quote.get("low")    or [],
        "close":                quote.get("close")  or [],
        "volume":               quote.get("volume") or [],
        "regular_market_price": meta.get("regularMarketPrice"),
    }


# ─── Math utilities ───────────────────────────────────────────────────────────

def is_finite(x) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


def mean(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else float("nan")


def std_sample(vals: list[float]) -> float:
    n = len(vals)
    if n < 2:
        return float("nan")
    m = mean(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / (n - 1))


def true_range(high: float, low: float, prev_close: float) -> float:
    if not (is_finite(high) and is_finite(low) and is_finite(prev_close)):
        return float("nan")
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr_last(highs: list, lows: list, closes: list, n: int) -> float:
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


# ─── Formatting utilities ─────────────────────────────────────────────────────

def fmtf(x, w: int, p: int) -> str:
    return f"{x:>{w}.{p}f}" if is_finite(x) else f"{'nan':>{w}}"


def fmt_price(x, width: int = 6, max_dp: int = 3) -> str:
    if not is_finite(x):
        return f"{'nan':>{width}}"
    for dp in range(max_dp, -1, -1):
        s = f"{x:.{dp}f}"
        if len(s) <= width:
            return s.rjust(width)
    s = f"{int(x):d}"
    if len(s) > width:
        s = s[:width]
    return s.rjust(width)


# ─── Symbol list utilities ────────────────────────────────────────────────────

_UNIVERSES_DIR = Path(__file__).parent / "symbols"


def load_auto_symbols(mode: str) -> list[str]:
    """Load symbols from symbols/all_<mode>_stocks.txt."""
    auto_file = _UNIVERSES_DIR / f"all_{mode}_stocks.txt"
    try:
        text = auto_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise FileNotFoundError(f"Auto symbols file not found: {auto_file}")
    symbols = text.split()
    if not symbols:
        raise ValueError(f"Auto symbols file contains no symbols: {auto_file}")
    return symbols


def normalize_symbol(mode: str, s: str) -> str:
    """Normalize a single symbol string for the given mode."""
    if mode == "sg":
        return ensure_si(s)
    if mode == "cc":
        return ensure_cc(s)
    return s.strip().upper()


def build_symbol_list(
    mode: str,
    input_symbols: list[str],
    exclude_symbols: list[str],
) -> list[str]:
    """
    Normalize symbols for the given mode, warn about duplicates, deduplicate
    (preserving first-occurrence order), and exclude the given exclusion set.
    """
    exclude_norm = {normalize_symbol(mode, s) for s in exclude_symbols}
    norm = [normalize_symbol(mode, s) for s in input_symbols]

    counts = Counter(norm)
    duplicates = [f"{sym} (x{counts[sym]})" for sym in counts if counts[sym] > 1]
    if duplicates:
        print("[WARN] Duplicate codes detected (will be de-duplicated): " + ", ".join(duplicates))

    seen: set = set()
    result: list[str] = []
    for sym in norm:
        if sym not in seen and sym not in exclude_norm:
            seen.add(sym)
            result.append(sym)
    return result


# ─── MR backtest core (shared by scan_mr_backtest and scan_technicals) ────────

def ts_to_date(ts) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def compute_lower_bb(closes: list[float], period: int = 20, num_std: float = 2.0) -> list:
    """Lower Bollinger Band for each bar; None for the first (period-1) bars (ddof=1)."""
    result = []
    for i in range(len(closes)):
        if i < period - 1:
            result.append(None)
        else:
            window = closes[i - period + 1 : i + 1]
            ma = mean(window)
            sd = std_sample(window)
            result.append(ma - num_std * sd if (is_finite(ma) and is_finite(sd)) else None)
    return result


def compute_z_arr(closes: list[float], period: int = 20) -> list:
    """Z-score (close − MA) / SD for each bar; None for the first (period-1) bars (ddof=1)."""
    result = []
    for i in range(len(closes)):
        if i < period - 1:
            result.append(None)
        else:
            window = closes[i - period + 1 : i + 1]
            ma = mean(window)
            sd = std_sample(window)
            result.append(
                (closes[i] - ma) / sd
                if (is_finite(ma) and is_finite(sd) and sd != 0)
                else None
            )
    return result


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
    Episode-based MR backtest driven by a Z-score threshold.

    Trigger  : close Z-score <= z_thres on day T, and (if delta_thres set)
               ΔLC% = 100*(close−MA20)/MA20 <= delta_thres.
    Entry    : next trading day (T+1); entry_price = open of T+1.
    TP price : entry_price * (1 + tp_level).
    WIN      : intraday high >= tp_price within max_hold trading days.
    FAIL     : max_hold elapsed without TP hit.
    OPEN     : data ended inside max_hold window with no TP hit.

    Reset rule: a new episode may only start after Z >= z_thres/10 has been
    observed at least once since the trigger day, preventing chained episodes
    inside a persistent downtrend.
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
            and o  is not None and is_finite(o)
        )
    ]

    if len(valid_days) < 20:
        raise ValueError(
            f"Only {len(valid_days)} valid bars — need at least 20 to compute BB(20,2)"
        )

    m         = len(valid_days)
    close_arr = [vd[2] for vd in valid_days]
    lb_arr    = compute_lower_bb(close_arr)
    z_arr     = compute_z_arr(close_arr)

    episodes: list[dict] = []
    i = 19  # first bar with valid 20-bar stats

    while i < m:
        z_i = z_arr[i]

        if z_i is None or not is_finite(z_i) or z_i > z_thres:
            i += 1
            continue

        trigger_i   = i
        entry_i     = trigger_i + 1
        if entry_i >= m:
            i += 1
            continue
        entry_price = valid_days[entry_i][5]  # open
        if entry_price is None or not is_finite(entry_price):
            i += 1
            continue

        lower_bb_entry = lb_arr[i]
        tp_price       = entry_price * (1 + tp_level)
        z_entry        = z_i
        ma20_trigger   = mean(close_arr[trigger_i - 19 : trigger_i + 1])
        lc_pct_entry   = (
            100.0 * (close_arr[trigger_i] - ma20_trigger) / ma20_trigger
            if (is_finite(ma20_trigger) and ma20_trigger != 0)
            else float("nan")
        )

        if delta_thres is not None:
            if not is_finite(lc_pct_entry) or lc_pct_entry > delta_thres:
                i += 1
                continue

        search_end = min(entry_i + max_hold, m)
        first_tp_k = None
        for k in range(entry_i, search_end):
            if valid_days[k][3] >= tp_price:
                first_tp_k = k
                break

        if first_tp_k is not None:
            outcome          = "win"
            tp_dur           = first_tp_k - entry_i
            fail_date        = None
            eventual_tp_date = None
            eventual_tp_dur  = None
            scan_end_low     = first_tp_k + 1
            i_next           = first_tp_k + 1
        elif entry_i + max_hold > m:
            outcome          = "open"
            tp_dur           = None
            fail_date        = None
            eventual_tp_date = None
            eventual_tp_dur  = None
            scan_end_low     = m
            i_next           = m
        else:
            outcome      = "fail"
            tp_dur       = None
            fail_date    = ts_to_date(valid_days[search_end - 1][1])
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
            # Low/high scan window: entry_date -> eventual TP date if TP was
            # eventually hit, otherwise entry_date -> latest available date.
            scan_end_low = eventual_tp_k + 1 if eventual_tp_k is not None else m

        scan_lows  = [valid_days[k][4] for k in range(entry_i, scan_end_low)]
        min_low    = min(scan_lows) if scan_lows else entry_price
        min_low_ts = next(
            (valid_days[k][1] for k in range(entry_i, scan_end_low) if valid_days[k][4] == min_low),
            None,
        ) if scan_lows else None

        scan_highs  = [valid_days[k][3] for k in range(entry_i, scan_end_low)]
        max_high    = max(scan_highs) if scan_highs else entry_price
        max_high_ts = next(
            (valid_days[k][1] for k in range(entry_i, scan_end_low) if valid_days[k][3] == max_high),
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
            "td_elapsed":         m - entry_i,
            "min_low":            min_low,
            "min_low_date":       ts_to_date(min_low_ts) if min_low_ts else None,
            "max_high":           max_high,
            "max_high_date":      ts_to_date(max_high_ts) if max_high_ts else None,
        })

        reset_level = z_thres / 10.0
        reset_met   = any(
            z_arr[k] is not None and is_finite(z_arr[k]) and z_arr[k] >= reset_level
            for k in range(trigger_i, i_next)
        )
        if reset_met:
            i = i_next
        else:
            i = i_next
            while i < m:
                z_j = z_arr[i]
                if z_j is not None and is_finite(z_j) and z_j >= reset_level:
                    break
                i += 1

    successes = [ep for ep in episodes if ep["outcome"] == "win"]
    last_ep   = episodes[-1] if episodes else None
    pending   = last_ep if (last_ep and last_ep["outcome"] == "open") else None

    rmp = chart.get("regular_market_price")
    lc  = rmp if is_finite(rmp) else valid_days[-1][2]

    today_ma20 = mean(close_arr[-20:]) if len(close_arr) >= 20 else float("nan")
    today_sd20 = std_sample(close_arr[-20:]) if len(close_arr) >= 20 else float("nan")
    today_z = (
        (lc - today_ma20) / today_sd20
        if is_finite(today_ma20) and is_finite(today_sd20) and today_sd20 != 0
        else float("nan")
    )
    today_lc_pct = (
        100.0 * (lc - today_ma20) / today_ma20
        if is_finite(today_ma20) and today_ma20 != 0
        else float("nan")
    )

    return {
        "symbol":       symbol,
        "name":         name,
        "n_episodes":   len(episodes),
        "data_start":   ts_to_date(valid_days[0][1]),
        "data_end":     ts_to_date(valid_days[-1][1]),
        "latest_close": lc,
        "today_z":      today_z,
        "today_lc_pct": today_lc_pct,
        "episodes":     episodes,
        "successes":    successes,
        "pending":      pending,
    }
