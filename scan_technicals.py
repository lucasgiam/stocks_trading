"""
scan_technicals.py

Scan SGX or US tickers on Yahoo and answer 15 yes/no (1/0) technical
questions per symbol, then report a total score per symbol.

Data is pulled the same way as scan_mr_ma20.py (Yahoo chart endpoint,
same cookie/crumb warm-up, same HTTP plumbing), but over a 2-year window
instead of 1-year, since several questions need a 200-day SMA trend
computed 20 trading days back (requires ~220 days of history), a
40-of-50-days-above-200-SMA check (requires ~250 days of history), plus
swing-low / volume / return checks over the most recent 20-50 days.

Usage examples:
  python scan_technicals.py --mode sg --symbols D05 C6L --sort_by score
  python scan_technicals.py --mode us --symbols AAPL MSFT NVDA --sort_by score delta
  python scan_technicals.py --mode us --symbols AAPL MSFT --delta_thres 0 --z_thres 0 --sort_by delta
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
- LC/MA20/MA50/MA200/ΔLC%/Z (shown in the output table alongside Score) are
  computed the same way as in scan_mr_ma20.py: ΔLC% = 100*(LC-MA20)/MA20,
  Z = (LC-MA20)/SD20 where SD20 is the 20-day sample standard deviation of
  closes.
- --delta_thres:
    * if X <= 0, keep rows where Delta% <= X
    * if X > 0, keep rows where Delta% > X
    * or set to 'z' to use per-record rule:
        - if Z <= 0 then Delta% <= Z
        - if Z > 0 then Delta% >= Z
- --z_thres:
    * if X <= 0, keep rows where Z <= X
    * if X > 0, keep rows where Z > X.
- --sort_by:
    'score' (default): sort rows by total score, descending. Optionally followed by a
             secondary tiebreaker key ('delta', 'z', or 'atr'; default 'atr'), e.g.
             '--sort_by score delta' sorts by score first, then ΔLC%. For 'delta'/'z'
             secondary keys, the tiebreak direction follows the same sign convention as
             the standalone --sort_by delta/z below (driven by --delta_thres/--z_thres).
             'atr' has no threshold and always breaks ties most-volatile-first.
    'delta': sort by ΔLC%; increasing (most negative first) unless delta_thres > 0,
             in which case decreasing (most positive first).
    'z':     sort by Z; increasing (most negative first) unless z_thres > 0,
             in which case decreasing (most positive first).
    'none': no sorting; keep scan order.
- The 15 questions (Q1-Q15):
    1. Broad market index is above its 200-day SMA.
    2. Broad market index's 200-day SMA is flat or rising over the past 20 days.
    3. Stock's current price is above its 200-day SMA.
    4. Stock's 200-day SMA is flat or rising over the past 20 days.
    5. Stock's 50-day SMA is above its 200-day SMA.
    6. Stock's daily closes were above its (trailing) 200-day SMA on at
       least 40 of the past 50 days.
    7. Stock's current price is above its lowest daily close in the past 20 days.
    8. Stock's current price is above the most recent confirmed swing-low
       close in the past 50 days. A swing low is a close that is lower than
       the 2 closes immediately before it and the 2 closes immediately after
       it (a 5-bar pivot); if no confirmed swing low exists in the window,
       this question is answered No.
    9. Stock has 3 or fewer high-volume down days in the past 20 days, where
       "high volume" means volume above the average volume of that same
       20-day window.
    10. Stock's total down-day volume does not exceed its total up-day volume
        by more than 50% in the past 20 days.
    11. Stock's latest 3-day return is better than the worst 3-day return
        observed in the past 20 days (rolling 3-day returns, ending on each
        of the last 20 trading days).
    12. Stock's latest close is within the upper half of its daily
        high-low range.
    13. Stock has adequate trading liquidity in the past 20 days, approximated
        from close/volume only (see LIQUIDITY_* constants below).
    14. Stock's 14-day ATR, expressed as a percent of current price, is at
        least ATR_PCT_MIN_THRESHOLD (see constant below).
    15. Stock's total return over the past 50 days is higher than the broad
        market index's total return over the same period.
- Liquidity (Q13) is not directly observable from chart data alone (no
  bid/ask spread is available from this endpoint), so it is approximated
  using the only two data series available - close and volume - over the
  past 20 trading days:
    * average daily dollar volume (close * volume) >= LIQUIDITY_MIN_DOLLAR_VOL, and
    * no more than LIQUIDITY_MAX_ILLIQUID_DAYS "illiquid days" in the past 20,
      where an illiquid day is one with zero volume or volume below 10% of
      the 20-day average volume (used as a proxy for thin/absent trading
      and, indirectly, wide bid-ask spreads).
  These thresholds are reasonable defaults, not externally specified; adjust
  LIQUIDITY_MIN_DOLLAR_VOL / LIQUIDITY_MAX_ILLIQUID_DAYS below if a different
  liquidity bar is wanted.
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

# ---------- Q8 swing-low pivot width (bars required on each side) ----------
SWING_LEFT_BARS = 2
SWING_RIGHT_BARS = 2

# ---------- Q13 liquidity thresholds (see module docstring) ----------
LIQUIDITY_MIN_DOLLAR_VOL = 200_000.0
LIQUIDITY_MAX_ILLIQUID_DAYS = 2

# ---------- Q14 ATR14% minimum threshold ----------
ATR_PCT_MIN_THRESHOLD = 4.0

# ---------- short column headers ("Q1".."Q15") for each of the 15 questions ----------
QUESTION_LABELS = {
    1: "Q1",    # broad index above its 200-SMA
    2: "Q2",    # broad index's 200-SMA flat or rising (past 20d)
    3: "Q3",    # stock price above its own 200-SMA
    4: "Q4",    # stock's 200-SMA flat or rising (past 20d)
    5: "Q5",    # stock's 50-SMA above its 200-SMA (golden-cross regime)
    6: "Q6",    # closes above trailing 200-SMA on >=40 of past 50 days
    7: "Q7",    # price above its lowest close in the past 20 days
    8: "Q8",    # price above most recent confirmed swing-low close (50d)
    9: "Q9",    # 3 or fewer high-volume down days in past 20 days
    10: "Q10",  # down-day volume doesn't exceed up-day volume by >50% (20d)
    11: "Q11",  # latest 3-day return beats the worst 3-day return (20d)
    12: "Q12",  # latest close in upper half of its daily high-low range
    13: "Q13",  # adequate trading liquidity (past 20 days)
    14: "Q14",  # ATR14% of price at least ATR_PCT_MIN_THRESHOLD
    15: "Q15",  # stock's 50d return beats the broad index's 50d return
}

# ---------- full wording of each of the 15 questions (printed above the table) ----------
QUESTION_TEXT = {
    1: "Relevant broad market index is above its 200-day SMA.",
    2: "Relevant broad market index's 200-day SMA is flat or rising.",
    3: "The stock's current price is above its 200-day SMA.",
    4: "The stock's 200-day SMA is flat or rising over the past 20 days.",
    5: "The stock's 50-day SMA is above its 200-day SMA.",
    6: "The stock's daily closes were above its 200-day SMA in at least 40 out of the past 50 days.",
    7: "The stock's current price is above its lowest daily close in the past 20 days.",
    8: "The stock's current price is above the most recent swing low close in the past 50 days.",
    9: "The stock has 3 or less high-volume down days in the past 20 days, where high-volume means volume above the 20-day average.",
    10: "The stock's total down-day volume does not exceed its total up-day volume by more than 50% in the past 20 days.",
    11: "The stock's latest 3-day return is better than the worst 3-day return in the past 20 days.",
    12: "The stock's latest close is within the upper half of its daily high-low range.",
    13: "The stock has adequate trading liquidity in the past 20 days, shown by meaningful average daily dollar volume and no evidence of persistently wide bid-ask spreads, unusually low trading volume, or frequent illiquid trading days.",
    14: "The stock's 14-day ATR divided by its current price is at least 4%.",
    15: "The stock total return is higher than the relevant broad market index's total return in the past 50 days.",
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


def mean(vals):
    return sum(vals) / len(vals) if vals else float("nan")


def std_sample(vals):
    n = len(vals)
    if n < 2:
        return float("nan")
    m = mean(vals)
    var = sum((x - m) ** 2 for x in vals) / (n - 1)
    return math.sqrt(var)


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


def find_swing_low_indices(closes, left=2, right=2):
    """Indices i where closes[i] is strictly lower than the `left` closes
    immediately before it and the `right` closes immediately after it
    (a confirmed pivot low)."""
    n = len(closes)
    idxs = []
    for i in range(left, n - right):
        c = closes[i]
        if not is_finite(c):
            continue
        neighbors = closes[i - left:i] + closes[i + 1:i + 1 + right]
        if not all(is_finite(x) for x in neighbors):
            continue
        if all(c < x for x in neighbors):
            idxs.append(i)
    return idxs


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
    """Derive all the per-instrument metrics needed across the 15 questions."""
    rows = build_rows(chart)
    closes = [r["close"] for r in rows]
    highs = [r["high"] if is_finite(r["high"]) else float("nan") for r in rows]
    lows = [r["low"] if is_finite(r["low"]) else float("nan") for r in rows]
    n = len(closes)

    rmp = chart.get("regular_market_price")
    price = rmp if is_finite(rmp) else (closes[-1] if closes else float("nan"))

    sma20_now = sma(closes, 20, n)
    sma200_now = sma(closes, 200, n)
    sma200_20ago = sma(closes, 200, n - 20) if n - 20 >= 0 else float("nan")
    sma50_now = sma(closes, 50, n)

    sd20 = std_sample(closes[-20:]) if n >= 20 else float("nan")

    if is_finite(sma20_now) and sma20_now != 0:
        delta_pct = 100.0 * (price - sma20_now) / sma20_now
    else:
        delta_pct = float("nan")

    if is_finite(price) and is_finite(sma20_now) and is_finite(sd20) and sd20 != 0:
        z = (price - sma20_now) / sd20
    else:
        z = float("nan")

    if n >= 51 and is_finite(closes[-1]) and is_finite(closes[-51]) and closes[-51] != 0:
        ret50 = closes[-1] / closes[-51] - 1.0
    else:
        ret50 = float("nan")

    atr14 = atr_last(highs, lows, closes, 14)
    atr14_pct = 100.0 * atr14 / price if is_finite(atr14) and is_finite(price) and price != 0 else float("nan")

    return {
        "rows": rows,
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "price": price,
        "sma20_now": sma20_now,
        "sma200_now": sma200_now,
        "sma200_20ago": sma200_20ago,
        "sma50_now": sma50_now,
        "sd20": sd20,
        "delta_pct": delta_pct,
        "z": z,
        "ret50": ret50,
        "atr14_pct": atr14_pct,
    }


def evaluate_questions(stock, idx):
    q = {}

    q[1] = is_finite(idx["price"]) and is_finite(idx["sma200_now"]) and idx["price"] > idx["sma200_now"]
    q[2] = is_finite(idx["sma200_now"]) and is_finite(idx["sma200_20ago"]) and idx["sma200_now"] >= idx["sma200_20ago"]

    q[3] = is_finite(stock["price"]) and is_finite(stock["sma200_now"]) and stock["price"] > stock["sma200_now"]
    q[4] = is_finite(stock["sma200_now"]) and is_finite(stock["sma200_20ago"]) and stock["sma200_now"] >= stock["sma200_20ago"]
    q[5] = is_finite(stock["sma50_now"]) and is_finite(stock["sma200_now"]) and stock["sma50_now"] > stock["sma200_now"]

    closes = stock["closes"]
    highs = stock["highs"]
    lows = stock["lows"]
    rows = stock["rows"]
    price = stock["price"]
    n = len(closes)

    # Q6: closes above trailing 200-SMA on >=40 of the past 50 days.
    if n >= 250:
        cnt = 0
        for i in range(n - 50, n):
            trailing_sma200 = sma(closes, 200, i + 1)
            if is_finite(closes[i]) and is_finite(trailing_sma200) and closes[i] > trailing_sma200:
                cnt += 1
        q[6] = cnt >= 40
    else:
        q[6] = False

    # Q7: price above lowest close in the past 20 days.
    if n >= 20:
        last20_closes = [c for c in closes[-20:] if is_finite(c)]
        low20 = min(last20_closes) if last20_closes else float("nan")
        q[7] = is_finite(price) and is_finite(low20) and price > low20
    else:
        q[7] = False

    # Q8: price above most recent confirmed swing-low close in the past 50 days.
    if n >= 50 + SWING_RIGHT_BARS:
        swing_idxs = find_swing_low_indices(closes, left=SWING_LEFT_BARS, right=SWING_RIGHT_BARS)
        recent_swings = [i for i in swing_idxs if i >= n - 50]
        if recent_swings:
            swing_low_close = closes[max(recent_swings)]
            q[8] = is_finite(price) and is_finite(swing_low_close) and price > swing_low_close
        else:
            q[8] = False
    else:
        q[8] = False

    # Q9: 3 or fewer high-volume down days in the past 20 days.
    if n >= 21:
        idxs20 = range(n - 20, n)
        vols20 = [rows[i]["volume"] for i in idxs20 if is_finite(rows[i]["volume"])]
        avg_vol20 = mean(vols20) if vols20 else float("nan")
        hv_down = 0
        for i in idxs20:
            prev_c, cur_c, vol = rows[i - 1]["close"], rows[i]["close"], rows[i]["volume"]
            if not (is_finite(prev_c) and is_finite(cur_c) and is_finite(vol) and is_finite(avg_vol20)):
                continue
            if vol > avg_vol20 and cur_c < prev_c:
                hv_down += 1
        q[9] = hv_down <= 3
    else:
        q[9] = False

    # Q10: down-day volume doesn't exceed up-day volume by more than 50% (past 20 days).
    if n >= 21:
        idxs20 = range(n - 20, n)
        up_vol = down_vol = 0.0
        any_valid = False
        for i in idxs20:
            prev_c, cur_c, vol = rows[i - 1]["close"], rows[i]["close"], rows[i]["volume"]
            if not (is_finite(prev_c) and is_finite(cur_c) and is_finite(vol)):
                continue
            any_valid = True
            if cur_c > prev_c:
                up_vol += vol
            elif cur_c < prev_c:
                down_vol += vol
        q[10] = any_valid and down_vol <= up_vol * 1.5
    else:
        q[10] = False

    # Q11: latest 3-day return better than the worst 3-day return in the past 20 days.
    if n >= 23:
        def ret3(i):
            if i - 3 < 0 or not (is_finite(closes[i]) and is_finite(closes[i - 3])) or closes[i - 3] == 0:
                return float("nan")
            return closes[i] / closes[i - 3] - 1.0

        window_rets = [r for r in (ret3(i) for i in range(n - 20, n)) if is_finite(r)]
        latest_ret3 = ret3(n - 1)
        if window_rets and is_finite(latest_ret3):
            q[11] = latest_ret3 > min(window_rets)
        else:
            q[11] = False
    else:
        q[11] = False

    # Q12: latest close within the upper half of its daily high-low range.
    if n >= 1 and is_finite(closes[-1]) and is_finite(highs[-1]) and is_finite(lows[-1]):
        h, l, c = highs[-1], lows[-1], closes[-1]
        q[12] = True if h <= l else (c - l) / (h - l) >= 0.5
    else:
        q[12] = False

    # Q13: adequate trading liquidity over the past 20 days.
    if n >= 20:
        last20_rows = rows[n - 20:n]
        dollar_vols = [
            r["close"] * r["volume"]
            for r in last20_rows
            if is_finite(r["close"]) and is_finite(r["volume"])
        ]
        vols_only = [r["volume"] for r in last20_rows if is_finite(r["volume"])]
        avg_dollar_vol = mean(dollar_vols) if dollar_vols else float("nan")
        avg_vol = mean(vols_only) if vols_only else float("nan")
        illiquid_days = 0
        for r in last20_rows:
            v = r["volume"]
            if not is_finite(v) or v == 0 or (is_finite(avg_vol) and avg_vol > 0 and v < 0.1 * avg_vol):
                illiquid_days += 1
        q[13] = (
            is_finite(avg_dollar_vol)
            and avg_dollar_vol >= LIQUIDITY_MIN_DOLLAR_VOL
            and illiquid_days <= LIQUIDITY_MAX_ILLIQUID_DAYS
        )
    else:
        q[13] = False

    # Q14: ATR14 as a percent of price is at least ATR_PCT_MIN_THRESHOLD.
    q[14] = is_finite(stock["atr14_pct"]) and stock["atr14_pct"] >= ATR_PCT_MIN_THRESHOLD

    # Q15: stock's 50-day return beats the broad index's 50-day return.
    q[15] = is_finite(stock["ret50"]) and is_finite(idx["ret50"]) and stock["ret50"] > idx["ret50"]

    return q


def main():
    ap = argparse.ArgumentParser(
        description="Scan SGX/US tickers (Yahoo) and score 15 technical yes/no questions per symbol."
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
        "--sort_by",
        nargs="+",
        default=["score"],
        metavar="MODE [SECONDARY]",
        help=(
            "'score' (default): sort rows by total score, descending. Optionally followed by a secondary "
            "tiebreaker key ('delta', 'z', or 'atr'; default 'atr'), e.g. '--sort_by score delta' sorts by "
            "score first, then ΔLC%% to break ties. For 'delta'/'z' secondary keys, tiebreak direction "
            "follows the corresponding delta_thres/z_thres sign (≤0 or unset → most negative first; >0 → "
            "most positive first); 'atr' has no threshold and always breaks ties most-volatile-first. "
            "'delta': sort by ΔLC%%; if delta_thres <= 0 or not specified → increasing (most negative first), "
            "if delta_thres > 0 → decreasing (most positive first). "
            "'z': sort by Z; if z_thres <= 0 or not specified → increasing (most negative first), "
            "if z_thres > 0 → decreasing (most positive first). "
            "'none': keep scan order."
        ),
    )
    ap.add_argument(
        "--score_thres",
        type=int,
        default=0,
        help="Minimum score required for a symbol to be shown in the output table (default 0, i.e. no filtering).",
    )
    # Accept float (as string) or the string 'z'
    ap.add_argument(
        "--delta_thres",
        default=None,
        help=(
            "Delta%% filter: if X <= 0, keep rows with Delta%% ≤ X; "
            "if X > 0, keep rows with Delta%% > X. "
            "Use 'z' to apply per-record rule: if Z ≤ 0 then Delta%% ≤ Z, else Delta%% ≥ Z."
        ),
    )
    ap.add_argument(
        "--z_thres",
        type=float,
        default=None,
        help=(
            "Z filter: if X <= 0, keep rows where Z ≤ X; "
            "if X > 0, keep rows where Z > X."
        ),
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.3,
        help="Seconds to sleep between requests.",
    )
    args = ap.parse_args()

    sort_mode = args.sort_by[0]
    if sort_mode not in ("none", "score", "delta", "z"):
        ap.error(f"--sort_by: invalid mode '{sort_mode}' (choose from none, score, delta, z)")
    sort_secondary = "atr"
    if len(args.sort_by) > 1:
        if sort_mode != "score":
            ap.error("--sort_by: a secondary key is only valid when the primary mode is 'score'")
        if len(args.sort_by) > 2:
            ap.error("--sort_by: at most two values are allowed (mode and secondary key)")
        sort_secondary = args.sort_by[1]
        if sort_secondary not in ("delta", "z", "atr"):
            ap.error(f"--sort_by: invalid secondary key '{sort_secondary}' (choose from delta, z, atr)")

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

    name_map = get_name_map(symbols)

    results = []
    for sym in tqdm(symbols, desc="Scanning", unit="symbol"):
        try:
            chart = fetch_chart_2y(sym)
            stock_struct = compute_struct(chart)
            q = evaluate_questions(stock_struct, idx_struct)
            score = sum(1 for v in q.values() if v)
            disp_code = sym.removesuffix(".SI") if args.mode == "sg" else sym
            results.append(
                {
                    "Symbol": disp_code,
                    "Name": name_map.get(sym, sym),
                    "Q": q,
                    "Score": score,
                    "LC": stock_struct["price"],
                    "MA20": stock_struct["sma20_now"],
                    "MA50": stock_struct["sma50_now"],
                    "MA200": stock_struct["sma200_now"],
                    "Delta%": stock_struct["delta_pct"],
                    "Z": stock_struct["z"],
                    "ATR14%": stock_struct["atr14_pct"],
                }
            )
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        finally:
            time.sleep(args.sleep)

    qs = list(range(1, 16))
    max_score = len(qs)

    filtered = [r for r in results if r["Score"] >= args.score_thres]

    applied = []

    # Apply optional Delta%/Z filters (same semantics as scan_mr_ma20.py)
    if args.delta_thres is not None:
        if isinstance(args.delta_thres, str) and args.delta_thres.lower() == "z":
            filtered = [
                r
                for r in filtered
                if is_finite(r.get("Z"))
                and is_finite(r.get("Delta%"))
                and (
                    (r["Z"] <= 0 and r["Delta%"] <= r["Z"])
                    or (r["Z"] > 0 and r["Delta%"] >= r["Z"])
                )
            ]
            applied.append("Delta% vs Z (per-record, sign-aware)")
        else:
            thr = float(args.delta_thres)
            if thr <= 0:
                filtered = [r for r in filtered if is_finite(r.get("Delta%")) and r["Delta%"] <= thr]
                applied.append(f"Delta% ≤ {thr:.2f}%")
            else:
                filtered = [r for r in filtered if is_finite(r.get("Delta%")) and r["Delta%"] > thr]
                applied.append(f"Delta% > {thr:.2f}%")
    if args.z_thres is not None:
        zt = float(args.z_thres)
        if zt <= 0:
            filtered = [r for r in filtered if is_finite(r.get("Z")) and r["Z"] <= zt]
            applied.append(f"Z ≤ {zt:.2f}")
        else:
            filtered = [r for r in filtered if is_finite(r.get("Z")) and r["Z"] > zt]
            applied.append(f"Z > {zt:.2f}")

    # ----- Sorting -----
    if sort_mode == "score":
        # Primary: Score descending. Tiebreaker: secondary key, direction following the
        # same delta_thres/z_thres sign convention as standalone --sort_by delta/z below
        # (negative or unset thres → most negative first; positive thres → most positive
        # first). 'atr' has no threshold, so it always breaks ties descending (most
        # volatile first), matching the previous default behavior.
        secondary_field = {"atr": "ATR14%", "delta": "Delta%", "z": "Z"}[sort_secondary]
        if sort_secondary == "atr":
            secondary_descending = True
        else:
            thr_arg = args.delta_thres if sort_secondary == "delta" else args.z_thres
            secondary_descending = False
            if thr_arg is not None and not (isinstance(thr_arg, str) and thr_arg.lower() == "z"):
                if float(thr_arg) > 0:
                    secondary_descending = True

        if secondary_descending:
            def secondary_key(r):
                v = r.get(secondary_field)
                return (0, -v) if is_finite(v) else (1, float("inf"))
        else:
            def secondary_key(r):
                v = r.get(secondary_field)
                return (0, v) if is_finite(v) else (1, float("inf"))

        # Stable two-pass sort: order by secondary key first, then by Score (descending);
        # ties on Score keep their relative secondary-key order from the first pass.
        filtered.sort(key=secondary_key)
        filtered.sort(key=lambda r: r["Score"], reverse=True)
    elif sort_mode in ("delta", "z"):
        metric_key = "Delta%" if sort_mode == "delta" else "Z"
        thr_arg = args.delta_thres if sort_mode == "delta" else args.z_thres
        descending = False
        if thr_arg is not None and not (isinstance(thr_arg, str) and thr_arg.lower() == "z"):
            if float(thr_arg) > 0:
                descending = True  # most positive first

        if not descending:
            def sort_key(r):
                v = r.get(metric_key)
                return (0, v) if is_finite(v) else (1, float("inf"))
        else:
            def sort_key(r):
                v = r.get(metric_key)
                return (0, -v) if is_finite(v) else (1, float("inf"))

        filtered.sort(key=sort_key)

    print(f"\nMode={args.mode} | Broad index={broad_index_symbol}")
    applied_str = "; ".join(applied) if applied else "no extra filters"
    print(
        f"Scored {len(results)} symbols out of {max_score} applicable questions, "
        f"{len(filtered)} passed score_thres >= {args.score_thres}"
        f"{' and ' + applied_str if applied else ''}.\n"
    )

    for n in qs:
        print(f"{QUESTION_LABELS[n]}: {QUESTION_TEXT[n]}")
    print()

    q_headers = " ".join(f"{QUESTION_LABELS[n]:>3}" for n in qs)
    header = (
        f"{'Code':<6} {'Name':<32} {q_headers} {'Score':>6} "
        f"{'LC':>6} {'MA20':>6} {'MA50':>6} {'MA200':>6} {'ΔLC%':>6} {'Z':>5} {'ATR%':>5}"
    )
    print(header)
    print("-" * len(header))

    for r in filtered:
        q_cells = " ".join(f"{1 if r['Q'][n] else 0:>3}" for n in qs)
        print(
            f"{(r['Symbol'] or '')[:6]:<6} "
            f"{(r['Name'] or '')[:32]:<32} "
            f"{q_cells} "
            f"{r['Score']:>3}/{max_score} "
            f"{fmt_price(r['LC'],   6)} "
            f"{fmt_price(r['MA20'], 6)} "
            f"{fmt_price(r['MA50'], 6)} "
            f"{fmt_price(r['MA200'], 6)} "
            f"{fmtf(r['Delta%'], 6, 2)} "
            f"{fmtf(r['Z'], 5, 2)} "
            f"{fmtf(r['ATR14%'], 5, 2)}"
        )

    if filtered:
        print()
        print("Symbol list:")
        print(" ".join(r["Symbol"] for r in filtered))
        print()


if __name__ == "__main__":
    main()
