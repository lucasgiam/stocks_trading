"""
Scan SGX tickers on Yahoo (.SI) against MA20.

Usage example:
python scan_ma20.py --symbols CC3 G13 N2IU C6L F34 BS6 Z74 O39 --threshold 0.05
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.error
import gzip

YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1mo&includeAdjustedClose=true"
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}"
YF_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search?q={symbol}&quotesCount=1"

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
    except urllib.error.HTTPError as e:
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

def mean(vals):
    return sum(vals) / len(vals) if vals else float("nan")

def main():
    ap = argparse.ArgumentParser(description="Filter SGX stocks by latest/MA20 ratio using Yahoo Finance.")
    ap.add_argument("--symbols", nargs="+", required=True, help="SGX codes with/without .SI (e.g. CC3 G13 N2IU C6L)")
    ap.add_argument("--threshold", type=float, default=0.05,
                    help="0.05 means +5%% above MA20 (ratio≥1.05). If ≥1.0, treated as ratio directly.")
    ap.add_argument("--sleep", type=float, default=0.3, help="Seconds to sleep between requests.")
    ap.add_argument("--names", choices=["auto","search","none"], default="auto",
                    help="How to fetch names: 'auto' (try quote then fallback), 'search' (per symbol), or 'none'.")
    args = ap.parse_args()

    ratio_threshold = 1.0 + args.threshold

    symbols_si = [ensure_si(s) for s in args.symbols]
    name_map = get_name_map(symbols_si, args.names)

    results = []
    for sym in symbols_si:
        try:
            last20, latest = fetch_last_20_and_latest(sym)
            ma20 = mean(last20)
            ratio = (latest / ma20) if ma20 else float("nan")
            results.append({"code": sym, "name": name_map.get(sym, sym),
                            "ma20": ma20, "latest": latest, "ratio": ratio})
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        time.sleep(args.sleep)

    filtered = [r for r in results if r.get("ratio", 0) >= ratio_threshold]
    filtered.sort(key=lambda x: x["ratio"], reverse=True)

    header = f"{'Code':<10} {'Name':<40} {'$MA20':>10} {'$Latest':>10} {'%Change':>8}"
    print(header); print("-" * len(header))
    if not filtered:
        print(f"(no matches with ratio >= {ratio_threshold:.4f})")
        return
    for r in filtered:
        print(f"{r['code']:<10} {r['name'][:38]:<40} {r['ma20']:>10.4f} {r['latest']:>10.4f} {(r['ratio']-1.0):>8.4f}")

if __name__ == "__main__":
    main()
