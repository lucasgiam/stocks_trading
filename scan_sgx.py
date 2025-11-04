"""
scan_sgx.py

Scan SGX tickers on Yahoo (.SI) and compute:
- LC (latest close)
- ΔLC% = 100 * (LC - MA20) / MA20
- MA20 / MA50 / MA100 / MA200
- SD20 (std dev of last 20 closes)
- Z-SD = (LC - MA20) / SD20
- ATR14  (Average True Range over last 14 days)
- Z-ATR = (LC - MA20) / ATR14
- RSI14 (Wilder)
- ΔRSI/D (linear regression slope of past 5 RSI14 values)
- R-sq% (R squared value of linear regression fit)
- D1Y% (Dividend yield based on past year)
- D5y% (Dividend yield based on past 5 years average)

Usage example:
  python scan_sgx.py --symbols CC3 G13 N2IU C6L --delta_thres z --div_thres 3 --z_thres -1

Notes:
- --symbols takes space-separated SGX codes (no quotes), with or without the ".SI" suffix.
- --delta_thres applies directly with its sign: Delta% must be <= delta_thres.
  Set to 'z' to use Delta% ≤ that record's Z-SD (per-record).
- --div_thres keeps only rows where Div1Y >= div_thres (independent of Div5Y).
- --z_thres applies directly with its sign: Z-SD must be <= z_thres.
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

# ========== USER INPUT ==========
DEFAULT_SYMBOLS = "[HTCD HBBD HCCD HBND HSHD HPCD HXXD HPAD HYDD D05 HSMD HMTD TDED O39 Z74 Z77 HJDD U11 HPPD K6S TADD S63 J36 Q0F TATD NIO C6L S68 F34 C38U H78 BN4 TPED TKKD TCPD A17U 9CI BS6 SO7 Y92 C07 U96 G13 IICD N2IU G07 5E2 U14 M44U C09 ME8U D01 AJBU EMI M04 T14 S58 J69U U06 V03 K71U TQ5 T82U S59 CJLU VC2 BUOU YF8 HMN E5H OV8 H02 C52 AIY A7RU EB5 C2PU S07 H15 U10 P8Z P7VU F17 NS8U 9A4U F99 CC3 8C8U TS0U BSL OYY BVA H22 JYEU CY6U A50 H13 AU8U Z25 NTDU AGS P40U SET F03 AP4 Q5T OU8 S41 ADN P15 O5RU G92 BEC W05 B61 J85 S61 558 S08 EH5 LJ3 T6I F9D CRPU P52 DCRU S20 STG U9E CHZ NC2 H07 RE4 QES E3B E28 BWM P9D O10 H30 AW9U B58 WJP C41 AWX V5Q 544 F83 T15 Q01 H18 8AZ S56 TSH P34 5JS M1GU QC7 U13 5TP 1D0 BHK F1E MZH BBW MV4 M01 5UF T24 5UX DHLU PCT 5IG ODBU B28 8U7U UD1U 5WJ S35 5GD Y03 MXNU 41O OXMU BN2 HQU NPW BDX S85 N02 5LY BTE JLB 5CF CMOU AWZ CLN TCU S3N 500 BTM 1MZ QNS ZKX KUO 5JK H12 5DD A30 DU4 G20 5VS ER0 BMGU 5WH J2T 5HV DM0 40T C33 HLS A04 X5N AWI Z59 XZL 5IC S7OU I07 BQM BQF BTOU BEW XJB L19 D03 42R 564 LVR M14 MR7 5ML RXS BTG T13 G0I A31 BLS T12 G50 5DP 579 C9Q 1L2 S23 L02 BPF F86 OAJ S19 5WA 1F2 Q0X PPC K75 WPC L38 S44 WKS BIP 1J5 5SO D5IU BTP BQD U77 N08 1J4 BCY 5MZ V7R 1E3 YYR YYY 41B Y35 NR7 O9E 42E BDR B69 40V 5SR URR 595 533 42L 566 RQ1 BNE ZB9 42C BKA BHU 5AE T41 B49 F13 5DM D8DU 546 BEZ S69 ZXY 42T C06 YK9 BBP 1D1 GEH 5WF KJ5 5G2 8K7 I49 T55 K29 M05 5DS C8R 1AZ 42W Y3D NEX 1Y1 A55 1B1 5I1 BKX 5UL 569 BEI S29 FQ7 53W S9B BIX 1F3 5EG LMS T43 C05 N01 AYN C76 9G2 1A1 O08 AWG 8YY I06 5PC 5GZ UIX 43A BFI L23 5TT N0Z 42F CHJ R14 P8A 5HH 541 5F7 YYN 554 BTJ 596 DRX LS9 1R6 Y8E 1V3 C13 VIN BQC SGR 5NV BQN CIN 5PD 5AB CNE OTX E27 BXE NPL AVX 532 5OI A33 GRQ 43B FRQ BKW 540 BFU 5KI 1H8 43Q BDU P36 5NF S71 C04 594 AOF K03 MIJ 505 543 AAJ 5AU CTO 5GI 5SY BQP OTS AJ2 5AL 1D4 XHV BAZ 1B0 A52 BJZ BCV VI2 AWC BRD BTX 5RA BJV 5AI KUX TVV E6R 5BI BFT 43E 42N 40W 5G1 MF6 5WV 5VP 5EV N32 504 1D5 CEDU 5FW 5VC PRH 570 5PF H20 1B6 TWL BHD BLZ 49B 5EB BFK 1H2 5DO SEJ ENV 5EF AZA F10 5G9 41F 5HG 583 5TJ 584 5IF BKZ QS9 BCZ M15 SES QZG OMK P74 J03 9QX 581 40N WJ9 5F4 5QY 5EW 5RC XCF YYB 9I7 NHD GU5 Y06 M03 V3M V8Y AWV 5OX 1D3 5UA 5G4 BLR 580 BAI BLU 43F 5FX AWK 585 5DX BKK 5CR I11 41T 8A1 KUH M11 1F0 CYW 5OR 1F1 BAC V2Y 5RE BKV 42Z 9VW LYY BEH E9L AWM AYV Z4D BJD]"
# ================================

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
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.8",
        "Connection": "keep-alive",
        "Referer": "https://finance.yahoo.com/",
        "Origin": "https://finance.yahoo.com",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    })
    with _OPENER.open(req, timeout=timeout) as resp:
        data = resp.read()
        text = _decompress_and_decode(resp, data)
        return json.loads(text)

def http_get_text(url, timeout=20):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.8",
        "Connection": "keep-alive",
        "Referer": "https://finance.yahoo.com/",
        "Origin": "https://finance.yahoo.com",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    })
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

def parse_symbols_string(s: str) -> list[str]:
    """Parse your DEFAULT_SYMBOLS format: '[CC3 G13 N2IU C6L]' -> ['CC3','G13','N2IU','C6L']"""
    if not s:
        return []
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    return [tok for tok in s.split() if tok]

def try_quote_names(symbols_si):
    """Fast path: quote endpoint for names."""
    name_map = {s: s for s in symbols_si}
    try:
        payload = http_get_json(YF_QUOTE_URL.format(symbols=",".join(symbols_si)))
        for q in payload.get("quoteResponse", {}).get("result", []):
            sym = q.get("symbol", "")
            nm = q.get("shortName") or q.get("longName") or q.get("displayName") or sym
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
            return quotes[0].get("shortname") or quotes[0].get("longname") or symbol_si
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

# ---- RSI(14) full series + slope & R^2 helpers ----
def rsi_wilder_series(closes, period=14):
    """Return list of RSI(period) values for the cleaned close series (None removed)."""
    cleaned = [c for c in closes if c is not None]
    if len(cleaned) < period + 1:
        return []
    diffs = [cleaned[i] - cleaned[i - 1] for i in range(1, len(cleaned))]
    gains = [max(d, 0.0) for d in diffs]
    losses = [max(-d, 0.0) for d in diffs]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    rsi_vals = []
    # first RSI after initial averages (at index `period`)
    if avg_loss == 0:
        rsi_vals.append(100.0 if avg_gain > 0 else 50.0)
    else:
        rs = avg_gain / avg_loss
        rsi_vals.append(100.0 - (100.0 / (1.0 + rs)))

    for i in range(period, len(diffs)):
        g = gains[i]
        l = losses[i]
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
        if avg_loss == 0:
            rsi = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
        rsi_vals.append(rsi)

    return rsi_vals

def linreg_slope_r2(y_vals):
    """Given y over equally spaced x=0..n-1, return (slope per day, R^2 as percent 0-100)."""
    n = len(y_vals)
    if n < 2:
        return (float("nan"), float("nan"))
    x_vals = list(range(n))
    mx = sum(x_vals) / n
    my = sum(y_vals) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(x_vals, y_vals))
    var_x = sum((x - mx) ** 2 for x in x_vals)
    var_y = sum((y - my) ** 2 for y in y_vals)
    if var_x == 0:
        return (float("nan"), float("nan"))
    slope = cov / var_x
    if var_x == 0 or var_y == 0:
        r2_pct = float("nan")
    else:
        r = cov / math.sqrt(var_x * var_y)
        r2_pct = (r * r) * 100.0
    return (slope, r2_pct)

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

    for summary_url in (YF_SUMMARY_URL_Q2 + "&crumb={crumb}", YF_SUMMARY_URL_Q1 + "&crumb={crumb}"):
        try:
            payload = http_get_json(summary_url.format(symbol=symbol_si, crumb=_CRUMB or ""))
            res = (payload.get("quoteSummary", {}) or {}).get("result", []) or [{}]
            d = res[0] if res else {}
            sd = d.get("summaryDetail", {}) or {}
            trailing = _raw(sd, "trailingAnnualDividendYield")
            if trailing is None:
                trailing = _raw(sd, "dividendYield")
            dy1 = float(trailing) * 100.0 if isinstance(trailing, (int, float)) else float("nan")
            dy5_raw = _raw(sd, "fiveYearAvgDividendYield")
            dy5 = float(dy5_raw) if isinstance(dy5_raw, (int, float)) else float("nan")
            return (dy1, dy5)
        except Exception as e:
            print(f"[WARN] {symbol_si}: quoteSummary attempt failed ({summary_url.split('/')[2]}): {e}", file=sys.stderr)
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
            print(f"[WARN] {symbol_si}: v7 quote attempt failed ({quote_url.split('/')[2]}): {e}", file=sys.stderr)
            time.sleep(0.2)

    print(f"[WARN] {symbol_si}: dividends fetch failed after all attempts", file=sys.stderr)
    return (float("nan"), float("nan"))

def is_finite(x):
    return isinstance(x, (int, float)) and math.isfinite(x)

# ---------- compact one-row table ----------
def fmtf(x, w, p):
    return f"{x:>{w}.{p}f}" if is_finite(x) else f"{'nan':>{w}}"

def main():
    ap = argparse.ArgumentParser(description="Scan SGX stocks (Yahoo) and rank by Delta% vs MA20.")
    ap.add_argument("--symbols", nargs="+", help="Space-separated SGX codes (e.g., CC3 G13 N2IU C6L). '.SI' optional.")
    # Accept float (as string) or the string 'z'
    ap.add_argument("--delta_thres", default=None,
                    help="Delta%% filter uses the exact value/sign you pass (Delta%% ≤ value). Use 'z' to apply per-record Delta%% ≤ Z-SD.")
    ap.add_argument("--div_thres", type=float, default=None,
                    help="Keep only rows where Div1Y >= div_thres.")
    ap.add_argument("--z_thres", type=float, default=None,
                    help="Z-SD filter uses the exact value/sign you pass (Z-SD ≤ value).")
    ap.add_argument("--sleep", type=float, default=0.3, help="Seconds to sleep between requests.")
    args = ap.parse_args()

    input_symbols = args.symbols if args.symbols else parse_symbols_string(DEFAULT_SYMBOLS)
    if not input_symbols:
        print("ERROR: No symbols provided via --symbols and DEFAULT_SYMBOLS is empty.")
        return

    normalized_symbols = [ensure_si(s) for s in input_symbols]
    counts = Counter(normalized_symbols)
    duplicates = [f"{sym} (x{counts[sym]})" for sym in counts if counts[sym] > 1]
    if duplicates:
        print("[WARN] Duplicate stock codes detected (will be de-duplicated): " + ", ".join(duplicates))

    symbols_si = list(dict.fromkeys(normalized_symbols))

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols_si[0])
    except Exception:
        pass

    name_map = get_name_map(symbols_si)

    results = []
    for sym in tqdm(symbols_si, desc="Scanning", unit="stock"):
        try:
            chart = fetch_chart_1y(sym)
            highs = chart["high"]
            lows  = chart["low"]
            closes = chart["close"]

            closes_valid = [c for c in closes if c is not None]
            if len(closes_valid) == 0:
                raise ValueError("No close prices in 1Y history")

            ma20  = ma_last(closes_valid, 20)
            ma50  = ma_last(closes_valid, 50)
            ma100 = ma_last(closes_valid, 100)
            ma200 = ma_last(closes_valid, 200)

            latest = latest_non_none(closes)
            std20  = std_pop(closes_valid[-20:]) if len(closes_valid) >= 1 else float("nan")
            atr14  = compute_atr14(highs, lows, closes)
            rsi14  = rsi_wilder_14(closes)

            # ---- RSI/T (slope) and R-sq over last 5 RSI14 values ----
            rsi_series = rsi_wilder_series(closes, period=14)
            if len(rsi_series) >= 5:
                last5 = rsi_series[-5:]  # oldest→newest by construction
                rsi_t, r_sq_pct = linreg_slope_r2(last5)
            else:
                rsi_t, r_sq_pct = (float("nan"), float("nan"))

            if is_finite(ma20) and ma20 != 0:
                delta_pct = 100.0 * (latest - ma20) / ma20
            else:
                delta_pct = float("nan")

            z_std = ((latest - ma20) / std20) if (is_finite(latest) and is_finite(ma20) and is_finite(std20) and std20 != 0) else float("nan")
            z_atr = ((latest - ma20) / atr14) if (is_finite(latest) and is_finite(ma20) and is_finite(atr14) and atr14 != 0) else float("nan")

            dy1, dy5 = fetch_div_yields(sym)

            results.append({
                "Symbol": sym.removesuffix(".SI"),
                "Name": name_map.get(sym, sym),
                "LC": latest,
                "MA20": ma20,
                "MA50": ma50,
                "MA100": ma100,
                "MA200": ma200,
                "Delta%": delta_pct,
                "STD20": std20,
                "Z-STD": z_std,
                "ATR14": atr14,
                "Z-ATR": z_atr,
                "RSI14": rsi14,
                "RSI/T": rsi_t,
                "R-sq": r_sq_pct, 
                "DivYield1Y": dy1,
                "DivYield5Y": dy5,
            })
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
                r for r in filtered
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
        filtered = [r for r in filtered if is_finite(r.get("Z-STD")) and r["Z-STD"] <= zt]
        applied.append(f"Z-SD ≤ {zt:.2f}")

    # Sort by increasing Delta%
    def sort_key(r):
        d = r.get("Delta%")
        return (0, d) if is_finite(d) else (1, float("inf"))
    filtered.sort(key=sort_key)

    # ==== Restored summary line ====
    applied_str = "; ".join(applied) if applied else "no extra filters"
    print(
        f"\nProcessed {len(results)} valid stocks, {len(filtered)} passed filter"
        f"{'s' if len(applied) > 1 else ''}: {applied_str}\n"
    )

    # ===== One-row compact table (short labels & widths) =====
    header = (
        f"{'Code':<4} {'Name':<9} "
        f"{'LC':>6} {'MA20':>6} {'MA50':>6} {'MA100':>6} {'MA200':>6} "
        f"{'ΔLC%':>5} {'SD20':>6} {'Z-SD':>5} {'ATR14':>6} {'Z-ATR':>5} "
        f"{'RSI14':>5} {'ΔR/D':>5} {'R-sq%':>5} {'D1Y%':>5} {'D5Y%':>5}"
    )
    print(header)
    print("-" * len(header))

    for r in filtered:
        print(
            f"{r['Symbol']:<4} "
            f"{(r['Name'] or '')[:9]:<9} "
            f"{fmtf(r['LC'],     6, 3)} "
            f"{fmtf(r['MA20'],   6, 3)} "
            f"{fmtf(r['MA50'],   6, 3)} "
            f"{fmtf(r['MA100'],  6, 3)} "
            f"{fmtf(r['MA200'],  6, 3)} "
            f"{fmtf(r['Delta%'], 5, 2)} "
            f"{fmtf(r['STD20'],  6, 3)} "
            f"{fmtf(r['Z-STD'],  5, 2)} "
            f"{fmtf(r['ATR14'],  6, 3)} "
            f"{fmtf(r['Z-ATR'],  5, 2)} "
            f"{fmtf(r['RSI14'],  5, 2)} "
            f"{fmtf(r['RSI/T'],  5, 2)} "
            f"{fmtf(r['R-sq'],   5, 2)} "
            f"{fmtf(r['DivYield1Y'], 5, 2)} "
            f"{fmtf(r['DivYield5Y'], 5, 2)}"
        )

        # Five-metric ordering line: largest → smallest
        metrics = [
            ("MA50", r.get("MA50")),
            ("MA20", r.get("MA20")),
            ("MA200", r.get("MA200")),
            ("MA100", r.get("MA100")),
            ("LC", r.get("LC")),
        ]

        def order_key_desc(item):
            val = item[1]
            return (0, -val) if is_finite(val) else (1, float("inf"))

        ordered = sorted(metrics, key=order_key_desc)
        ordering_str = " > ".join([m[0] for m in ordered])
        print(f"({ordering_str})")

if __name__ == "__main__":
    main()
