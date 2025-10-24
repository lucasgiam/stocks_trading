"""
Scan SGX tickers on Yahoo (.SI) against MA20.
Stocks whose last close price dropped below the MA20 by at least the threshold percentage will be shown in the output.

Usage example:
    python scan_ma20.py --symbols CC3 G13 N2IU C6L F34 BS6 Z74 O39 --price_thres 5.0 --div_thres 3.0

If --symbols is omitted, the script will use DEFAULT_SYMBOLS defined below, formatted like: [CC3 G13 N2IU C6L]
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.error
import gzip
from tqdm import tqdm
from collections import Counter

# ========== USER INPUT ==========
DEFAULT_SYMBOLS = "[HTCD HBBD HCCD HBND HSHD HPCD HXXD HPAD HYDD D05 HSMD HMTD TDED O39 Z74 Z77 HJDD U11 HPPD K6S TADD S63 J36 Q0F TATD NIO C6L S68 F34 C38U H78 BN4 TPED TKKD TCPD A17U 9CI BS6 SO7 Y92 C07 U96 G13 IICD N2IU G07 5E2 U14 M44U C09 ME8U D01 AJBU EMI M04 T14 S58 J69U U06 V03 K71U TQ5 T82U S59 CJLU VC2 BUOU YF8 HMN E5H OV8 H02 C52 AIY A7RU EB5 C2PU S07 H15 U10 P8Z P7VU F17 NS8U 9A4U F99 CC3 8C8U TS0U BSL OYY BVA H22 JYEU CY6U A50 H13 AU8U Z25 NTDU AGS P40U SET F03 AP4 Q5T OU8 S41 ADN P15 O5RU G92 BEC W05 B61 J85 S61 558 S08 EH5 LJ3 T6I F9D CRPU P52 DCRU S20 STG U9E CHZ NC2 H07 RE4 QES E3B E28 BWM P9D O10 H30 AW9U B58 WJP C41 AWX V5Q 544 F83 T15 Q01 H18 8AZ S56 TSH P34 5JS M1GU QC7 U13 5TP 1D0 BHK F1E MZH BBW MV4 M01 5UF T24 5UX DHLU PCT 5IG ODBU B28 8U7U UD1U 5WJ S35 5GD Y03 MXNU 41O OXMU BN2 HQU NPW BDX S85 N02 5LY BTE JLB 5CF CMOU AWZ CLN TCU S3N 500 BTM 1MZ QNS ZKX KUO 5JK H12 5DD A30 DU4 G20 5VS ER0 BMGU 5WH J2T 5HV DM0 40T C33 HLS A04 X5N AWI Z59 XZL 5IC S7OU I07 BQM BQF BTOU BEW XJB L19 D03 42R 564 LVR M14 MR7 5ML RXS BTG T13 G0I A31 BLS T12 G50 5DP 579 C9Q 1L2 S23 L02 BPF F86 OAJ S19 5WA 1F2 Q0X PPC K75 WPC L38 S44 WKS BIP 1J5 5SO D5IU BTP BQD U77 N08 1J4 BCY 5MZ V7R 1E3 YYR YYY 41B Y35 NR7 O9E 42E BDR B69 40V 5SR URR 595 533 42L 566 RQ1 BNE ZB9 42C BKA BHU 5AE T41 B49 F13 5DM D8DU 546 BEZ S69 ZXY 42T C06 YK9 BBP 1D1 GEH 5WF KJ5 5G2 8K7 I49 T55 K29 M05 5DS C8R 1AZ 42W Y3D NEX 1Y1 A55 1B1 5I1 BKX 5UL 569 BEI S29 FQ7 53W S9B BIX 1F3 5EG LMS T43 C05 N01 AYN C76 9G2 1A1 O08 AWG 8YY I06 5PC 5GZ UIX 43A BFI L23 5TT N0Z 42F CHJ BDA R14 P8A 5HH 541 5F7 YYN 554 BTJ 596 DRX LS9 1R6 Y8E 1V3 C13 VIN BQC SGR 5NV BQN CIN 5PD 5AB CNE OTX E27 BXE NPL AVX 532 5OI A33 GRQ 43B FRQ BKW 540 BFU 5KI 1H8 43Q BDU P36 5NF S71 C04 594 AOF K03 MIJ 505 543 AAJ 5AU CTO 5GI 5SY BQP OTS AJ2 5AL 1D4 XHV BAZ 1B0 A52 BJZ BCV VI2 AWC BRD BTX 5RA BJV 5AI KUX TVV E6R 5BI BFT 43E 42N 40W 5G1 MF6 5WV 5VP 5EV N32 504 1D5 CEDU 5FW 5VC PRH 570 5PF H20 1B6 TWL BHD BLZ 49B 5EB BFK 1H2 5DO SEJ ENV 5EF AZA F10 5G9 41F 5HG 583 5TJ 584 5IF BKZ QS9 BCZ M15 SES QZG OMK P74 J03 9QX 581 40N WJ9 5F4 5QY 5EW 5RC XCF YYB 1H3 9I7 NHD GU5 Y06 M03 V3M V8Y AWV 5OX 1D3 5UA 5G4 BLR 580 BAI BLU 43F 5FX AWK 585 5DX BKK 5CR I11 41T 8A1 KUH M11 1F0 CYW 5OR 1F1 BAC V2Y 5RE BKV 42Z 9VW LYY BEH E9L AWM AYV Z4D BJD]"
# ================================

YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1mo&includeAdjustedClose=true"
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}"
YF_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search?q={symbol}&quotesCount=1"
YF_CHART_DIV_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    "?period1={p1}&period2={p2}&interval=1d&events=div&includeAdjustedClose=true"
)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

def http_get_json(url, timeout=15):
    # Request gzip (avoid brotli) and decompress if needed
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "application/json, text/javascript,*/*;q=0.1",
        "Accept-Encoding": "gzip",   # no "br"
        "Accept-Language": "en-US,en;q=0.8",
        "Connection": "close",
        "Referer": "https://finance.yahoo.com/",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        enc = (resp.headers.get("Content-Encoding") or "").lower()
        if enc == "gzip" or (len(data) > 2 and data[:2] == b"\x1f\x8b"):
            data = gzip.decompress(data)
        return json.loads(data.decode("utf-8"))

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

# Fetch and sum dividends in the last 365 days (by ex-div date)
def fetch_dividends_sum_1y(symbol_si) -> float:
    now = int(time.time())
    p1 = now - 365 * 24 * 60 * 60
    payload = http_get_json(YF_CHART_DIV_URL.format(symbol=symbol_si, p1=p1, p2=now))
    result = payload.get("chart", {}).get("result", [])
    if not result:
        return 0.0
    events = result[0].get("events", {}) or {}
    divs = events.get("dividends") or {}
    total = 0.0
    # Keys are timestamps; items contain {"amount": x, "date": ts, ...}
    for _k, v in divs.items():
        try:
            amt = v.get("amount")
            ts = v.get("date") or v.get("timestamp")
            if amt is None:
                continue
            # Range already bounded by period1/2, but be defensive:
            if isinstance(ts, int):
                if p1 <= ts <= now:
                    total += float(amt)
            else:
                total += float(amt)
        except Exception:
            continue
    return total

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
    ap.add_argument("--div_thres", type=float, default=0.0,
                    help="Minimum dividend yield in percent to include (e.g., 3.0 means ≥ 3%).")
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

    name_map = get_name_map(symbols_si, args.names)

    results = []
    for sym in tqdm(symbols_si, desc="Scanning", unit="stock"):
        try:
            last20, latest = fetch_last_20_and_latest(sym)
            ma20 = mean(last20)
            ratio = (latest / ma20) if ma20 else float("nan")

            # --- dividends and yield (TTM-by-365d window) ---
            try:
                div1y = fetch_dividends_sum_1y(sym)
                yield_pct = (div1y / latest * 100.0) if latest else float("nan")
            except Exception as e:
                print(f"[WARN] {sym}: dividends fetch failed: {e}", file=sys.stderr)
                div1y = float("nan")
                yield_pct = float("nan")

            results.append({
                "code": sym,
                "name": name_map.get(sym, sym),
                "ma20": ma20,
                "latest": latest,
                "ratio": ratio,
                "div1y": div1y,          # total dividends per share in last 365 days
                "yield_pct": yield_pct,  # div1y / latest * 100
            })
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        time.sleep(args.sleep)

    # Filter by price drop vs MA20
    filtered = [r for r in results if r.get("ratio", 0) <= ratio_threshold]
    # Filter out watch list names
    filtered = [r for r in filtered if not is_watch_list_name(r.get("name", ""))]
    # Filter by dividend yield percent threshold
    filtered = [r for r in filtered if r.get("yield_pct", float("nan")) >= args.div_thres]

    filtered.sort(key=lambda x: x["ratio"])

    # Print stats
    print(f"\nProcessed {len(results)} valid stocks, {len(filtered)} passed filter (price dropped by at least {100*(1-ratio_threshold):.2f}% and yield ≥ {args.div_thres:.2f}%).\n")

    header = f"{'Code':<10} {'Name':<30} {'$MA20':>10} {'$Latest':>10} {'%Change':>8} {'$Div1Y':>10} {'%Yield':>8}"
    if not filtered:
        return

    print(header); print("-" * len(header))
    for r in filtered:
        print(
            f"{r['code']:<10} "
            f"{r['name'][:28]:<30} "
            f"{r['ma20']:>10.4f} "
            f"{r['latest']:>10.4f} "
            f"{(100*(r['ratio']-1.0)):>8.2f} "
            f"{r.get('div1y', float('nan')):>10.4f} "
            f"{r.get('yield_pct', float('nan')):>8.2f}"
        )

if __name__ == "__main__":
    main()
