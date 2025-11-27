"""
scan_stocks.py

Scan SGX or US tickers on Yahoo and compute:
- LC (latest close)
- MA20 (20-day moving average)
- MA±X% = (1 + X/100) * MA20, where X = pct_offset (default = -4)
- MA±XSD = MA20 + X * SD20, where X = z_offset (default = 0)
- ΔLC% = 100 * (LC - MA20) / MA20
- SD20 (std dev of last 20 closes)
- Z-SD = (LC - MA20) / SD20
- ATR14 (Average True Range over last 14 days)
- Z-ATR = (LC - MA20) / ATR14
- RSI14 (Wilder's Relative Strength Index over last 14 days)
- D1Y% (Dividend yield based on past year)
- D5Y% (Dividend yield based on past 5 years average)

Usage example:
  python scan_stocks.py --mode sg --symbols CC3 G13 N2IU C6L --delta_thres 0 --div_thres 3 --z_thres 0
  python scan_stocks.py --mode us --symbols AAPL GOOG MSFT --delta_thres 0

Notes:
- --mode selects SGX ('sg') or US ('us') tickers. In 'sg' mode, tickers are mapped to Yahoo by appending '.SI'.
- --symbols takes space-separated stock codes (no quotes). For SGX, codes like 'D05', 'C6L' ('.SI' optional). For US, codes like 'AAPL', 'GOOG'.
- --delta_thres applies directly with its sign: Delta% must be <= delta_thres, set to 'z' to use Delta% ≤ that record's Z-SD (per-record).
- --div_thres keeps only rows where Div1Y >= div_thres (independent of Div5Y).
- --z_thres applies directly with its sign: Z-SD must be <= z_thres.
- --pct_offset controls the MA20 offset X (in %) used to compute MA±X%.
- --z_offset controls the MA20 offset X (in standard deviations) used to compute MA±XSD.
- --exclude removes the specified symbols from being processed
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
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    "?interval=1d&range=1y&includeAdjustedClose=true"
)

# quoteSummary for dividends (1Y trailing & 5Y avg)
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


def compute_atr14(highs, lows, closes):
    """ATR(14) with True Range computed from H/L/PrevClose; ignores None rows."""
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
    last14 = tr_list[-14:] if len(tr_list) >= 14 else tr_list
    return mean(last14)


def rsi_wilder_14(closes):
    """Wilder's RSI(14); returns NaN if insufficient data."""
    if len(closes) < 15:
        return float("nan")
    diffs = []
    prev = None
    for c in closes:
        if c is None:
            continue
        if prev is None:
            prev = c
            continue
        diffs.append(c - prev)
        prev = c
    if len(diffs) < 14:
        return float("nan")
    gains = [max(d, 0.0) for d in diffs]
    losses = [max(-d, 0.0) for d in diffs]
    avg_gain = sum(gains[:14]) / 14.0
    avg_loss = sum(losses[:14]) / 14.0
    for i in range(14, len(diffs)):
        g = gains[i]
        l = losses[i]
        avg_gain = (avg_gain * 13 + g) / 14.0
        avg_loss = (avg_loss * 13 + l) / 14.0
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def latest_non_none(arr):
    for x in reversed(arr):
        if x is not None:
            return x
    return float("nan")


# ===== Dividend yields =====
def fetch_div_yields(symbol_si):
    """
    Return (DivYield1Y_pct, DivYield5Y_pct) directly from Yahoo summaryDetail:
      - DivYield1Y: prefer trailingAnnualDividendYield; fallback to dividendYield
      - DivYield5Y: fiveYearAvgDividendYield
    """

    def _raw(x, k):
        if not isinstance(x, dict):
            return None
        v = x.get(k)
        return v.get("raw") if isinstance(v, dict) else v

    if _CRUMB is None and not _CJ._cookies:
        warm_up_cookies_and_crumb(symbol_si)

    for summary_url in (
        YF_SUMMARY_URL_Q2 + "&crumb={crumb}",
        YF_SUMMARY_URL_Q1 + "&crumb={crumb}",
    ):
        try:
            payload = http_get_json(
                summary_url.format(symbol=symbol_si, crumb=_CRUMB or "")
            )
            res = (payload.get("quoteSummary", {}) or {}).get("result", []) or [{}]
            d = res[0] if res else {}
            sd = d.get("summaryDetail", {}) or {}
            trailing = _raw(sd, "trailingAnnualDividendYield")
            if trailing is None:
                trailing = _raw(sd, "dividendYield")
            dy1 = (
                float(trailing) * 100.0
                if isinstance(trailing, (int, float))
                else float("nan")
            )
            dy5_raw = _raw(sd, "fiveYearAvgDividendYield")
            dy5 = (
                float(dy5_raw)
                if isinstance(dy5_raw, (int, float))
                else float("nan")
            )
            return (dy1, dy5)
        except Exception as e:
            print(
                f"[WARN] {symbol_si}: quoteSummary attempt failed ({summary_url.split('/')[2]}): {e}",
                file=sys.stderr,
            )
            time.sleep(0.2)

    for quote_url in (YF_QUOTE_URL, YF_QUOTE_URL_ALT):
        try:
            qpayload = http_get_json(quote_url.format(symbols=symbol_si))
            results = (qpayload.get("quoteResponse", {}) or {}).get("result", [])
            if not results:
                continue
            q = results[0]
            dy = q.get("trailingAnnualDividendYield", None)
            if dy is None:
                dy = q.get("dividendYield", None)
            dy1 = (dy * 100.0) if isinstance(dy, (int, float)) else float("nan")
            dy5 = q.get("fiveYearAvgDividendYield", None)
            dy5 = float(dy5) if isinstance(dy5, (int, float)) else float("nan")
            return (dy1, dy5)
        except Exception as e:
            print(
                f"[WARN] {symbol_si}: v7 quote attempt failed ({quote_url.split('/')[2]}): {e}",
                file=sys.stderr,
            )
            time.sleep(0.2)

    print(
        f"[WARN] {symbol_si}: dividends fetch failed after all attempts",
        file=sys.stderr,
    )
    return (float("nan"), float("nan"))


def is_finite(x):
    return isinstance(x, (int, float)) and math.isfinite(x)


# ---------- compact one-row table ----------
def fmtf(x, w, p):
    return f"{x:>{w}.{p}f}" if is_finite(x) else f"{'nan':>{w}}"


def main():
    ap = argparse.ArgumentParser(
        description="Scan SGX or US stocks (Yahoo) and rank by Delta% vs MA20."
    )
    ap.add_argument(
        "--mode",
        choices=["sg", "us"],
        required=True,
        help=(
            "Market mode: 'sg' for SGX (codes like D05, C6L; '.SI' will be appended), "
            "'us' for US stocks (codes like AAPL, GOOG; used as-is)."
        ),
    )
    ap.add_argument(
        "--symbols",
        nargs="+",
        help=(
            "Space-separated stock codes (e.g., CC3 G13 for SGX; AAPL GOOG for US). "
            "For SGX, '.SI' suffix is optional."
        ),
    )
    # Accept float (as string) or the string 'z'
    ap.add_argument(
        "--delta_thres",
        default=None,
        help=(
            "Delta% filter uses the exact value/sign you pass (Delta% ≤ value). "
            "Use 'z' to apply per-record Delta% ≤ Z-SD."
        ),
    )
    ap.add_argument(
        "--div_thres",
        type=float,
        default=None,
        help="Keep only rows where Div1Y >= div_thres.",
    )
    ap.add_argument(
        "--z_thres",
        type=float,
        default=None,
        help="Z-SD filter uses the exact value/sign you pass (Z-SD ≤ value).",
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
        help="Space-separated stock codes to exclude ('.SI' optional for SGX).",
    )
    ap.add_argument(
        "--pct_offset",
        type=float,
        default=-4.0,
        help="Offset X in percent used to compute MA±X% relative to MA20 (default -4).",
    )
    ap.add_argument(
        "--z_offset",
        type=float,
        default=0.0,
        help=(
            "Offset X in standard deviations used to compute MA±XSD "
            "relative to MA20 (default 0)."
        ),
    )
    args = ap.parse_args()

    if not args.symbols:
        print(
            "ERROR: No symbols provided. Please supply at least one via --symbols.",
            file=sys.stderr,
        )
        return

    input_symbols = args.symbols

    exclude_symbols = args.exclude if args.exclude else []

    if args.mode == "sg":
        exclude_normalized = {ensure_si(s) for s in exclude_symbols}
        normalized_symbols = [ensure_si(s) for s in input_symbols]
    else:  # 'us'
        exclude_normalized = {s.strip().upper() for s in exclude_symbols}
        normalized_symbols = [s.strip().upper() for s in input_symbols]

    counts = Counter(normalized_symbols)
    duplicates = [f"{sym} (x{counts[sym]})" for sym in counts if counts[sym] > 1]
    if duplicates:
        print(
            "[WARN] Duplicate stock codes detected (will be de-duplicated): "
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
    ma20_factor = 1.0 + args.pct_offset / 100.0

    for sym in tqdm(symbols_si, desc="Scanning", unit="stock"):
        try:
            chart = fetch_chart_1y(sym)
            highs = chart["high"]
            lows = chart["low"]
            closes = chart["close"]

            closes_valid = [c for c in closes if c is not None]
            if len(closes_valid) == 0:
                raise ValueError("No close prices in 1Y history")

            ma20 = ma_last(closes_valid, 20)

            latest = latest_non_none(closes)
            std20 = (
                std_pop(closes_valid[-20:])
                if len(closes_valid) >= 1
                else float("nan")
            )
            atr14 = compute_atr14(highs, lows, closes)
            rsi14 = rsi_wilder_14(closes)
            if is_finite(ma20) and is_finite(std20):
                ma20_sd = ma20 + args.z_offset * std20
            else:
                ma20_sd = float("nan")

            if is_finite(ma20) and ma20 != 0:
                delta_pct = 100.0 * (latest - ma20) / ma20
            else:
                delta_pct = float("nan")

            z_std = (
                (latest - ma20) / std20
                if (
                    is_finite(latest)
                    and is_finite(ma20)
                    and is_finite(std20)
                    and std20 != 0
                )
                else float("nan")
            )
            z_atr = (
                (latest - ma20) / atr14
                if (
                    is_finite(latest)
                    and is_finite(ma20)
                    and is_finite(atr14)
                    and atr14 != 0
                )
                else float("nan")
            )

            dy1, dy5 = fetch_div_yields(sym)
            ma20_m4 = ma20 * ma20_factor if is_finite(ma20) else float("nan")

            results.append(
                {
                    "Symbol": sym.removesuffix(".SI"),
                    "Name": name_map.get(sym, sym),
                    "LC": latest,
                    "MA20": ma20,
                    "MA20m4": ma20_m4,
                    "MA_SD": ma20_sd,
                    "Delta%": delta_pct,
                    "STD20": std20,
                    "Z-STD": z_std,
                    "ATR14": atr14,
                    "Z-ATR": z_atr,
                    "RSI14": rsi14,
                    "DivYield1Y": dy1,
                    "DivYield5Y": dy5,
                }
            )
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        finally:
            time.sleep(args.sleep)

    # Base set: drop rows where Delta% isn't computable (needs MA20)
    filtered = [r for r in results if is_finite(r.get("Delta%"))]

    # Apply optional filters
    applied = []
    if args.delta_thres is not None:
        # 'z' mode: keep rows where Delta% ≤ that record's Z-SD (per-record)
        if isinstance(args.delta_thres, str) and args.delta_thres.lower() == "z":
            filtered = [
                r
                for r in filtered
                if is_finite(r.get("Z-STD")) and r["Delta%"] <= r["Z-STD"]
            ]
            applied.append("Delta% ≤ Z-SD (per-record)")
        else:
            thr = float(args.delta_thres)
            filtered = [r for r in filtered if r["Delta%"] <= thr]
            applied.append(f"Delta% ≤ {thr:.2f}%")
    if args.div_thres is not None:
        dv = float(args.div_thres)

        def keep_div1y(r):
            d1 = r.get("DivYield1Y", float("nan"))
            return is_finite(d1) and (d1 >= dv)

        filtered = [r for r in filtered if keep_div1y(r)]
        applied.append(f"Div1Y ≥ {dv:.2f}%")
    if args.z_thres is not None:
        zt = float(args.z_thres)
        filtered = [
            r
            for r in filtered
            if is_finite(r.get("Z-STD")) and r["Z-STD"] <= zt
        ]
        applied.append(f"Z-SD ≤ {zt:.2f}")

    # Sort by increasing Delta%
    def sort_key(r):
        d = r.get("Delta%")
        return (0, d) if is_finite(d) else (1, float("inf"))

    filtered.sort(key=sort_key)

    # ==== Summary line ====
    applied_str = "; ".join(applied) if applied else "no extra filters"
    print(
        f"\nProcessed {len(results)} valid stocks, {len(filtered)} passed filter"
        f"{'s' if len(applied) > 1 else ''}: {applied_str}\n"
    )

    # ===== One-row compact table (short labels & widths) =====
    pct_label = f"MA{args.pct_offset:+.0f}%"
    z_label = f"MA{args.z_offset:+.0f}SD"
    header = (
        f"{'Code':<4} {'Name':<10} "
        f"{'LC':>7} {'MA20':>7} {pct_label:>7} {z_label:>7} "
        f"{'ΔLC%':>6} {'SD20':>6} {'Z-SD':>5} {'ATR14':>6} {'Z-ATR':>5} "
        f"{'RSI14':>5} {'D1Y%':>5} {'D5Y%':>5}"
    )
    print(header)
    print("-" * len(header))

    for r in filtered:
        print(
            f"{r['Symbol']:<4} "
            f"{(r['Name'] or '')[:10]:<10} "
            f"{fmtf(r['LC'],       7, 3)} "
            f"{fmtf(r['MA20'],     7, 3)} "
            f"{fmtf(r['MA20m4'],   7, 3)} "
            f"{fmtf(r['MA_SD'],    7, 3)} "
            f"{fmtf(r['Delta%'],   6, 2)} "
            f"{fmtf(r['STD20'],    6, 3)} "
            f"{fmtf(r['Z-STD'],    5, 2)} "
            f"{fmtf(r['ATR14'],    6, 3)} "
            f"{fmtf(r['Z-ATR'],    5, 2)} "
            f"{fmtf(r['RSI14'],    5, 2)} "
            f"{fmtf(r['DivYield1Y'], 5, 2)} "
            f"{fmtf(r['DivYield5Y'], 5, 2)}"
        )


if __name__ == "__main__":
    main()
