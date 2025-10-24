"""
Scan SGX tickers on Yahoo (.SI) against MA20.
Stocks whose last close price dropped below the MA20 by at least the threshold percentage will be shown in the output.

Usage example:
    python scan_ma20.py --symbols CC3 G13 N2IU C6L F34 BS6 Z74 O39 --price_thres 5.0 --div_thres 1 --pe_thres 20 --npm_thres 5 --incl_nan True

If --symbols is omitted, the script will use DEFAULT_SYMBOLS defined below, formatted like: [CC3 G13 N2IU C6L]
"""

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
from tqdm import tqdm
from collections import Counter
from __future__ import annotations

try:
    import brotli  # optional
except Exception:
    brotli = None

# ========== USER INPUT ==========
DEFAULT_SYMBOLS = "[HTCD HBBD HCCD HBND HSHD HPCD HXXD HPAD HYDD D05 HSMD HMTD TDED O39 Z74 Z77 HJDD U11 HPPD K6S TADD S63 J36 Q0F TATD NIO C6L S68 F34 C38U H78 BN4 TPED TKKD TCPD A17U 9CI BS6 SO7 Y92 C07 U96 G13 IICD N2IU G07 5E2 U14 M44U C09 ME8U D01 AJBU EMI M04 T14 S58 J69U U06 V03 K71U TQ5 T82U S59 CJLU VC2 BUOU YF8 HMN E5H OV8 H02 C52 AIY A7RU EB5 C2PU S07 H15 U10 P8Z P7VU F17 NS8U 9A4U F99 CC3 8C8U TS0U BSL OYY BVA H22 JYEU CY6U A50 H13 AU8U Z25 NTDU AGS P40U SET F03 AP4 Q5T OU8 S41 ADN P15 O5RU G92 BEC W05 B61 J85 S61 558 S08 EH5 LJ3 T6I F9D CRPU P52 DCRU S20 STG U9E CHZ NC2 H07 RE4 QES E3B E28 BWM P9D O10 H30 AW9U B58 WJP C41 AWX V5Q 544 F83 T15 Q01 H18 8AZ S56 TSH P34 5JS M1GU QC7 U13 5TP 1D0 BHK F1E MZH BBW MV4 M01 5UF T24 5UX DHLU PCT 5IG ODBU B28 8U7U UD1U 5WJ S35 5GD Y03 MXNU 41O OXMU BN2 HQU NPW BDX S85 N02 5LY BTE JLB 5CF CMOU AWZ CLN TCU S3N 500 BTM 1MZ QNS ZKX KUO 5JK H12 5DD A30 DU4 G20 5VS ER0 BMGU 5WH J2T 5HV DM0 40T C33 HLS A04 X5N AWI Z59 XZL 5IC S7OU I07 BQM BQF BTOU BEW XJB L19 D03 42R 564 LVR M14 MR7 5ML RXS BTG T13 G0I A31 BLS T12 G50 5DP 579 C9Q 1L2 S23 L02 BPF F86 OAJ S19 5WA 1F2 Q0X PPC K75 WPC L38 S44 WKS BIP 1J5 5SO D5IU BTP BQD U77 N08 1J4 BCY 5MZ V7R 1E3 YYR YYY 41B Y35 NR7 O9E 42E BDR B69 40V 5SR URR 595 533 42L 566 RQ1 BNE ZB9 42C BKA BHU 5AE T41 B49 F13 5DM D8DU 546 BEZ S69 ZXY 42T C06 YK9 BBP 1D1 GEH 5WF KJ5 5G2 8K7 I49 T55 K29 M05 5DS C8R 1AZ 42W Y3D NEX 1Y1 A55 1B1 5I1 BKX 5UL 569 BEI S29 FQ7 53W S9B BIX 1F3 5EG LMS T43 C05 N01 AYN C76 9G2 1A1 O08 AWG 8YY I06 5PC 5GZ UIX 43A BFI L23 5TT N0Z 42F CHJ R14 P8A 5HH 541 5F7 YYN 554 BTJ 596 DRX LS9 1R6 Y8E 1V3 C13 VIN BQC SGR 5NV BQN CIN 5PD 5AB CNE OTX E27 BXE NPL AVX 532 5OI A33 GRQ 43B FRQ BKW 540 BFU 5KI 1H8 43Q BDU P36 5NF S71 C04 594 AOF K03 MIJ 505 543 AAJ 5AU CTO 5GI 5SY BQP OTS AJ2 5AL 1D4 XHV BAZ 1B0 A52 BJZ BCV VI2 AWC BRD BTX 5RA BJV 5AI KUX TVV E6R 5BI BFT 43E 42N 40W 5G1 MF6 5WV 5VP 5EV N32 504 1D5 CEDU 5FW 5VC PRH 570 5PF H20 1B6 TWL BHD BLZ 49B 5EB BFK 1H2 5DO SEJ ENV 5EF AZA F10 5G9 41F 5HG 583 5TJ 584 5IF BKZ QS9 BCZ M15 SES QZG OMK P74 J03 9QX 581 40N WJ9 5F4 5QY 5EW 5RC XCF YYB 9I7 NHD GU5 Y06 M03 V3M V8Y AWV 5OX 1D3 5UA 5G4 BLR 580 BAI BLU 43F 5FX AWK 585 5DX BKK 5CR I11 41T 8A1 KUH M11 1F0 CYW 5OR 1F1 BAC V2Y 5RE BKV 42Z 9VW LYY BEH E9L AWM AYV Z4D BJD]"
# ================================

YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1mo&includeAdjustedClose=true"
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}&lang=en-US&region=US"
YF_QUOTE_URL_ALT = "https://query2.finance.yahoo.com/v7/finance/quote?symbols={symbols}&lang=en-US&region=US"
YF_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search?q={symbol}&quotesCount=1"
YF_CHART_DIV_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    "?period1={p1}&period2={p2}&interval=1d&events=div&includeAdjustedClose=true"
)
YF_SUMMARY_URL_Q2 = (
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    "?modules=summaryDetail,defaultKeyStatistics,financialData,price"
    "&formatted=false&lang=en-US&region=US&ssl=true&corsDomain=finance.yahoo.com"
)
YF_SUMMARY_URL_Q1 = (
    "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    "?modules=summaryDetail,defaultKeyStatistics,financialData,price"
    "&formatted=false&lang=en-US&region=US&ssl=true&corsDomain=finance.yahoo.com"
)
YF_QUOTE_PAGE = "https://finance.yahoo.com/quote/{symbol}?p={symbol}"
YF_KEY_STATS_PAGE = "https://finance.yahoo.com/quote/{symbol}/key-statistics?p={symbol}"
YF_HOME = "https://finance.yahoo.com/"
YF_GET_CRUMB = "https://query1.finance.yahoo.com/v1/test/getcrumb"

# Use a very modern UA (some servers gate by UA families)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

# Reusable opener with cookies
_CJ = cookielib.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_CJ))
_CRUMB = None  # filled by warmup()

def _decompress_and_decode(resp, data: bytes) -> str:
    enc = (resp.headers.get("Content-Encoding") or "").lower()
    if enc == "br" and brotli is not None:
        data = brotli.decompress(data)
    elif enc == "gzip" or (len(data) > 2 and data[:2] == b"\x1f\x8b"):
        data = gzip.decompress(data)
    elif enc == "deflate":
        data = zlib.decompress(data, -zlib.MAX_WBITS)
    return data.decode("utf-8", errors="replace")

def http_get_json(url, timeout=15):
    # add crumb if placeholder present
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

def http_get_text(url, timeout=15):
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
    """
    Visit homepage and a quote page to set consent cookies, then fetch crumb.
    """
    global _CRUMB
    try:
        _ = http_get_text(YF_HOME)
        time.sleep(0.3)
        _ = http_get_text(YF_QUOTE_PAGE.format(symbol=symbol_si_for_visit))
        time.sleep(0.3)
        # Try to get crumb
        try:
            crumb_text = http_get_text(YF_GET_CRUMB).strip()
            if crumb_text and len(crumb_text) < 64:
                _CRUMB = crumb_text
        except Exception as e:
            print(f"[INFO] crumb fetch failed: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[INFO] warm-up failed: {e}", file=sys.stderr)

def ensure_si(ticker: str) -> str:
    t = ticker.strip().upper()
    return t if t.endswith(".SI") else f"{t}.SI"

def try_quote_names(symbols_si):
    """Best-effort name map via v7 quote. May 401; caller should handle exceptions."""
    payload = http_get_json(YF_QUOTE_URL.format(symbols=",".join(symbols_si)))
    name_map = {}
    for q in payload.get("quoteResponse", {}).get("result", []):
        sym = q.get("symbol", "")
        name = q.get("shortName") or q.get("longName") or q.get("displayName") or sym
        name_map[sym] = name
    return name_map

def try_search_name(symbol_si):
    """Fallback per-symbol: search endpoint often returns shortname."""
    try:
        p = http_get_json(YF_SEARCH_URL.format(symbol=symbol_si))
        quotes = p.get("quotes", [])
        if quotes:
            return quotes[0].get("shortname") or quotes[0].get("longname") or symbol_si
    except Exception:
        pass
    return symbol_si

def get_name_map(symbols_si, names_mode):
    # names_mode: "auto" (default), "search", or "none"
    name_map = {s: s for s in symbols_si}
    if names_mode == "none":
        return name_map
    if names_mode == "search":
        for s in symbols_si:
            name_map[s] = try_search_name(s)
        return name_map
    # auto: try quote API first, fall back to search per symbol
    try:
        nm = try_quote_names(symbols_si)
        for s in symbols_si:
            name_map[s] = nm.get(s, s)
        return name_map
    except urllib.error.HTTPError:
        # 401/403: fall back to search per symbol
        for s in symbols_si:
            name_map[s] = try_search_name(s)
        return name_map
    except Exception:
        # any other error -> fallback per symbol
        for s in symbols_si:
            name_map[s] = try_search_name(s)
        return name_map

def fetch_last_20_and_latest(symbol_si):
    payload = http_get_json(YF_CHART_URL.format(symbol=symbol_si))
    result = payload.get("chart", {}).get("result", [])
    if not result:
        raise ValueError("No chart result")
    r = result[0]
    closes = (r.get("indicators", {}).get("quote", [{}])[0].get("close") or [])
    closes = [c for c in closes if c is not None]
    if not closes:
        raise ValueError("No close prices")
    latest = closes[-1]
    last20 = closes[-20:] if len(closes) >= 20 else closes
    return last20, latest

# --- helpers for scraping JSON from HTML ---
_JSON_BLOCK_RE = re.compile(r'"QuoteSummaryStore"\s*:\s*{', re.IGNORECASE)

def _extract_json_object_following(html: str, start_idx: int) -> dict | None:
    """
    Given index of opening '{' of a JSON object in a string, extract the object
    by balancing braces, then json.loads it.
    """
    if start_idx < 0 or start_idx >= len(html):
        return None
    brace_start = html.find("{", start_idx)
    if brace_start == -1:
        return None
    depth = 0
    end = -1
    for i in range(brace_start, len(html)):
        ch = html[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end == -1:
        return None
    blob = html[brace_start:end]
    # clean potential JS-specific tokens if any (rare)
    try:
        return json.loads(blob)
    except Exception:
        # Try to fix common issues: remove trailing commas
        cleaned = re.sub(r",\s*([}\]])", r"\1", blob)
        try:
            return json.loads(cleaned)
        except Exception:
            return None

# --- fetch fundamentals ---
def fetch_fundamentals(symbol_si):
    """
    Returns a dict with:
      pe_ttm, pe_fwd, div_yield_pct, div_yield_5y_pct, profit_margin_pct
    Missing values are float('nan').
    """
    def _nan_result():
        return {
            "pe_ttm": float("nan"),
            "pe_fwd": float("nan"),
            "div_yield_pct": float("nan"),
            "div_yield_5y_pct": float("nan"),
            "profit_margin_pct": float("nan"),
        }

    def _as_raw(x, k):
        v = x.get(k)
        if isinstance(v, dict):
            return v.get("raw")
        return v

    # Warm up cookies/crumb once per run (first symbol is fine)
    if _CRUMB is None and not _CJ._cookies:
        warm_up_cookies_and_crumb(symbol_si)

    # 1) Try quoteSummary (query2 then query1), append crumb if present
    for summary_url in (YF_SUMMARY_URL_Q2 + "&crumb={crumb}", YF_SUMMARY_URL_Q1 + "&crumb={crumb}"):
        try:
            payload = http_get_json(summary_url.format(symbol=symbol_si, crumb=_CRUMB or ""))
            res = (payload.get("quoteSummary", {}) or {}).get("result", []) or [{}]
            d = res[0] if res else {}
            sd = d.get("summaryDetail", {}) or {}
            ks = d.get("defaultKeyStatistics", {}) or {}
            fd = d.get("financialData", {}) or {}
            price = d.get("price", {}) or {}

            pe_ttm = (
                _as_raw(sd, "trailingPE")
                or _as_raw(ks, "trailingPE")
                or _as_raw(price, "trailingPE")
            )
            pe_fwd = _as_raw(sd, "forwardPE") or _as_raw(ks, "forwardPE")

            div_yield = _as_raw(sd, "dividendYield")
            if div_yield is None:
                div_yield = _as_raw(sd, "trailingAnnualDividendYield")
            div_yield_pct = (div_yield * 100.0) if isinstance(div_yield, (int, float)) else float("nan")

            # fiveYearAvgDividendYield appears to be already in percent on Yahoo
            div_yield_5y = _as_raw(sd, "fiveYearAvgDividendYield")
            div_yield_5y_pct = float(div_yield_5y) if isinstance(div_yield_5y, (int, float)) else float("nan")

            profit_margin = _as_raw(fd, "profitMargins")
            profit_margin_pct = (profit_margin * 100.0) if isinstance(profit_margin, (int, float)) else float("nan")

            return {
                "pe_ttm": float(pe_ttm) if isinstance(pe_ttm, (int, float)) else float("nan"),
                "pe_fwd": float(pe_fwd) if isinstance(pe_fwd, (int, float)) else float("nan"),
                "div_yield_pct": div_yield_pct,
                "div_yield_5y_pct": div_yield_5y_pct,
                "profit_margin_pct": profit_margin_pct,
            }
        except Exception as e:
            print(f"[INFO] {symbol_si}: quoteSummary attempt failed ({summary_url.split('/')[2]}): {e}", file=sys.stderr)
            time.sleep(0.2)

    # 2) Fallback: v7 quote (query1 then query2)
    for quote_url in (YF_QUOTE_URL, YF_QUOTE_URL_ALT):
        try:
            qpayload = http_get_json(quote_url.format(symbols=symbol_si))
            results = (qpayload.get("quoteResponse", {}) or {}).get("result", [])
            if not results:
                continue
            q = results[0]

            pe_ttm = q.get("trailingPE")
            pe_fwd = q.get("forwardPE")

            dy = q.get("dividendYield", None)
            if dy is None:
                dy = q.get("trailingAnnualDividendYield", None)
            div_yield_pct = (dy * 100.0) if isinstance(dy, (int, float)) else float("nan")

            # fiveYearAvgDividendYield is already in percent here as well
            dy5 = q.get("fiveYearAvgDividendYield", None)
            div_yield_5y_pct = float(dy5) if isinstance(dy5, (int, float)) else float("nan")

            pm = q.get("profitMargins", None)
            profit_margin_pct = (pm * 100.0) if isinstance(pm, (int, float)) else float("nan")

            return {
                "pe_ttm": float(pe_ttm) if isinstance(pe_ttm, (int, float)) else float("nan"),
                "pe_fwd": float(pe_fwd) if isinstance(pe_fwd, (int, float)) else float("nan"),
                "div_yield_pct": div_yield_pct,
                "div_yield_5y_pct": div_yield_5y_pct,
                "profit_margin_pct": profit_margin_pct,
            }
        except Exception as e:
            print(f"[INFO] {symbol_si}: v7 quote attempt failed ({quote_url.split('/')[2]}): {e}", file=sys.stderr)
            time.sleep(0.2)

    # 3) Final fallback: scrape quote and key-statistics pages and parse QuoteSummaryStore
    for page_url in (YF_QUOTE_PAGE, YF_KEY_STATS_PAGE):
        try:
            html = http_get_text(page_url.format(symbol=symbol_si))
            m = _JSON_BLOCK_RE.search(html)
            if not m:
                # try a looser match around window.YAHOO.Finance regions
                m = re.search(r'("stores"?\s*:\s*{[^}]*"QuoteSummaryStore"\s*:\s*{)', html, re.IGNORECASE | re.DOTALL)
            if m:
                start_idx = m.start()
                store = _extract_json_object_following(html, start_idx)
                if store:
                    sd = store.get("summaryDetail", {}) or {}
                    ks = store.get("defaultKeyStatistics", {}) or {}
                    fd = store.get("financialData", {}) or {}
                    price = store.get("price", {}) or {}

                    def _raw(x, k):
                        v = x.get(k)
                        if isinstance(v, dict):
                            return v.get("raw")
                        return v

                    pe_ttm = (
                        _raw(sd, "trailingPE")
                        or _raw(ks, "trailingPE")
                        or _raw(price, "trailingPE")
                    )
                    pe_fwd = _raw(sd, "forwardPE") or _raw(ks, "forwardPE")

                    div_yield = _raw(sd, "dividendYield")
                    if div_yield is None:
                        div_yield = _raw(sd, "trailingAnnualDividendYield")
                    div_yield_pct = (div_yield * 100.0) if isinstance(div_yield, (int, float)) else float("nan")

                    # fiveYearAvgDividendYield already in percent
                    div_yield_5y = _raw(sd, "fiveYearAvgDividendYield")
                    div_yield_5y_pct = float(div_yield_5y) if isinstance(div_yield_5y, (int, float)) else float("nan")

                    profit_margin = _raw(fd, "profitMargins")
                    profit_margin_pct = (profit_margin * 100.0) if isinstance(profit_margin, (int, float)) else float("nan")

                    return {
                        "pe_ttm": float(pe_ttm) if isinstance(pe_ttm, (int, float)) else float("nan"),
                        "pe_fwd": float(pe_fwd) if isinstance(pe_fwd, (int, float)) else float("nan"),
                        "div_yield_pct": div_yield_pct,
                        "div_yield_5y_pct": div_yield_5y_pct,
                        "profit_margin_pct": profit_margin_pct,
                    }
            else:
                print(f"[INFO] {symbol_si}: QuoteSummaryStore not found in {page_url}", file=sys.stderr)
        except Exception as e:
            print(f"[INFO] {symbol_si}: scrape failed ({page_url.split('/')[2]}): {e}", file=sys.stderr)
        time.sleep(0.2)

    # If everything fails:
    print(f"[WARN] {symbol_si}: fundamentals fetch failed after all attempts", file=sys.stderr)
    return _nan_result()

def mean(vals):
    return sum(vals) / len(vals) if vals else float("nan")

def is_watch_list_name(name: str) -> bool:
    return "- watch list" in (name or "").lower()

def parse_symbols_string(s: str) -> list[str]:
    """
    Accepts a string like "[CC3 G13 N2IU C6L]" or "CC3 G13 N2IU C6L" and returns
    ['CC3','G13','N2IU','C6L'].
    """
    if not s:
        return []
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    return [tok for tok in s.split() if tok]

def main():
    ap = argparse.ArgumentParser(description="Filter SGX stocks by latest/MA20 ratio using Yahoo Finance.")
    ap.add_argument("--symbols", nargs="+", help="SGX codes with/without .SI (e.g. CC3 G13 N2IU C6L)")
    ap.add_argument("--price_thres", type=float, default=5.0,
                    help="Price threshold in PERCENT (e.g., 5.0 means latest ≤ MA20 - 5%).")
    ap.add_argument("--div_thres", type=float, default=1.0,
                    help="Dividend threshold in PERCENT. Keep only if min(DivY%, DivY5Y%) > div_thres.")
    ap.add_argument("--pe_thres", type=float, default=20.0,
                    help="P/E threshold (unitless). Keep only if max(PE_TTM, PE_Fwd) < pe_thres.")
    ap.add_argument("--npm_thres", type=float, default=5.0,
                    help="Net profit margin threshold in PERCENT. Keep only if NPM% ≥ npm_thres.")
    ap.add_argument("--incl_nan", type=lambda s: s.lower() in ("true", "1", "yes", "y"),
                    default=False,
                    help="If True, NaN/inf values are allowed to pass filters. If False (default), NaN/inf fails.")
    ap.add_argument("--sleep", type=float, default=0.3, help="Seconds to sleep between requests.")
    ap.add_argument("--names", choices=["auto","search","none"], default="auto",
                    help="How to fetch names: 'auto' (try quote then fallback), 'search' (per symbol), or 'none'.")
    args = ap.parse_args()

    # --- choose CLI symbols if provided, otherwise use DEFAULT_SYMBOLS ---
    input_symbols = args.symbols if args.symbols else parse_symbols_string(DEFAULT_SYMBOLS)
    if not input_symbols:
        print("ERROR: No symbols provided via --symbols and DEFAULT_SYMBOLS is empty.")
        return

    # Interpret price_thres as percent
    ratio_threshold = 1.0 - (args.price_thres / 100.0)

    # Normalize symbols to .SI and uppercase, detect duplicates, and de-duplicate while preserving order
    normalized_symbols = [ensure_si(s) for s in input_symbols]
    counts = Counter(normalized_symbols)
    duplicates = [f"{sym} (x{counts[sym]})" for sym in counts if counts[sym] > 1]
    if duplicates:
        print("[WARN] Duplicate stock codes detected (will be de-duplicated): " + ", ".join(duplicates))

    # Unique list in original order
    symbols_si = list(dict.fromkeys(normalized_symbols))

    print("[INFO] Fetching scanning data...")

    # Warm up once with the first symbol to establish cookies/crumb
    try:
        warm_up_cookies_and_crumb(symbols_si[0])
    except Exception:
        pass

    name_map = get_name_map(symbols_si, args.names)

    results = []
    for sym in tqdm(symbols_si, desc="Scanning", unit="stock"):
        try:
            last20, latest = fetch_last_20_and_latest(sym)
            ma20 = mean(last20)
            ratio = (latest / ma20) if ma20 else float("nan")

            f = fetch_fundamentals(sym)

            results.append({
                "code": sym,
                "name": name_map.get(sym, sym),
                "ma20": ma20,
                "latest": latest,
                "ratio": ratio,
                "pe_ttm": f["pe_ttm"],
                "pe_fwd": f["pe_fwd"],
                "div_yield_pct": f["div_yield_pct"],
                "div_yield_5y_pct": f["div_yield_5y_pct"],
                "profit_margin_pct": f["profit_margin_pct"],
            })
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        time.sleep(args.sleep)

    # Filter by price drop vs MA20
    filtered = [r for r in results if r.get("ratio", 0) <= ratio_threshold]
    # Filter out watch list names
    filtered = [r for r in filtered if not is_watch_list_name(r.get("name", ""))]

    # --- FILTERS (respect --incl_nan) ---
    def is_bad(x):  # True if NaN or inf or not a number
        return not (isinstance(x, (int, float)) and math.isfinite(x))

    def dividend_ok(r):
        dy = r.get("div_yield_pct", float("nan"))
        dy5 = r.get("div_yield_5y_pct", float("nan"))
        if args.incl_nan and (is_bad(dy) or is_bad(dy5)):
            return True
        if is_bad(dy) or is_bad(dy5):
            return False
        return min(dy, dy5) > args.div_thres

    def pe_ok(r):
        pe1 = r.get("pe_ttm", float("nan"))
        pe2 = r.get("pe_fwd", float("nan"))
        if args.incl_nan and (is_bad(pe1) or is_bad(pe2)):
            return True
        if is_bad(pe1) or is_bad(pe2):
            return False
        return max(pe1, pe2) < args.pe_thres

    def npm_ok(r):
        npm = r.get("profit_margin_pct", float("nan"))
        if args.incl_nan and is_bad(npm):
            return True
        if is_bad(npm):
            return False
        return npm >= args.npm_thres

    filtered = [r for r in filtered if dividend_ok(r) and pe_ok(r) and npm_ok(r)]
    # ------------------------------------

    filtered.sort(key=lambda x: x["ratio"])

    # Print stats
    print(
        f"\nProcessed {len(results)} valid stocks, {len(filtered)} passed filter "
        f"(price ≤ MA20 - {args.price_thres:.2f}%, "
        f"min(DivY,5Y) > {args.div_thres:.2f}%, "
        f"max(PEs) < {args.pe_thres:.2f}, "
        f"NPM ≥ {args.npm_thres:.2f}%"
        f"{' (NaN allowed)' if args.incl_nan else ' (NaN excluded)'}).\n"
    )

    header = (
        f"{'Code':<10} {'Name':<25} {'$MA20':>10} {'$Latest':>10} {'%Change':>8} "
        f"{'PE_TTM':>8} {'PE_Fwd':>8} {'DivY%':>8} {'DivY5Y%':>9} {'NPM%':>8}"
    )
    if not filtered:
        return

    print(header); print("-" * len(header))
    for r in filtered:
        print(
            f"{r['code']:<10} "
            f"{r['name'][:23]:<25} "
            f"{r['ma20']:>10.4f} "
            f"{r['latest']:>10.4f} "
            f"{(100*(r['ratio']-1.0)):>8.2f} "
            f"{r.get('pe_ttm', float('nan')):>8.2f} "
            f"{r.get('pe_fwd', float('nan')):>8.2f} "
            f"{r.get('div_yield_pct', float('nan')):>8.2f} "
            f"{r.get('div_yield_5y_pct', float('nan')):>9.2f} "
            f"{r.get('profit_margin_pct', float('nan')):>8.2f}"
        )

if __name__ == "__main__":
    main()
