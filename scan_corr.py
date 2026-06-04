"""
scan_corr.py

Fetch daily prices from Yahoo Finance for a list of symbols and compute a Pearson correlation matrix
on daily log returns over the past 200 trading days.

Computation:
- Uses close price as returned by Yahoo Finance.
- Daily log returns: r_t = ln(P_t / P_{t-1})
- Uses the most recent 200 trading days of returns (requires 201 common price points).
- Pearson correlation on log returns.

Output:
- Displays a correlation heatmap (matrix) in the same order as the input symbols list.
- Color scale: -1 = red, 0 = white, +1 = blue.

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

import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, Normalize
from tqdm import tqdm

# Yahoo endpoints
YF_HOME = "https://finance.yahoo.com/"
YF_QUOTE_PAGE = "https://finance.yahoo.com/quote/{symbol}?p={symbol}"

YF_CHART_1Y_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?"
    "interval=1d&range=1y"
)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
_CJ = cookielib.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_CJ))


def _decompress_and_decode(resp, data: bytes) -> str:
    enc = (resp.headers.get("Content-Encoding") or "").lower()
    if enc == "gzip" or (len(data) > 2 and data[:2] == b"\x1f\x8b"):
        data = gzip.decompress(data)
    elif enc == "deflate":
        data = zlib.decompress(data, -zlib.MAX_WBITS)
    return data.decode("utf-8", errors="replace")


def http_get_json(url, timeout=20):
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


def warm_up(symbol_for_visit: str):
    """Make Yahoo happy (cookies)."""
    try:
        _ = http_get_text(YF_HOME)
        time.sleep(0.3)
        _ = http_get_text(YF_QUOTE_PAGE.format(symbol=symbol_for_visit))
        time.sleep(0.3)
    except Exception:
        pass


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


def is_finite(x) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


def fetch_price_series_1y(symbol: str):
    """Return (timestamps, close_list) from Yahoo chart API."""
    payload = http_get_json(YF_CHART_1Y_URL.format(symbol=symbol))
    result = payload.get("chart", {}).get("result", []) or []
    if not result:
        raise ValueError("No chart result")
    r0 = result[0]
    ts = r0.get("timestamp") or []
    quote = ((r0.get("indicators", {}) or {}).get("quote", [{}]) or [{}])[0]
    return ts, quote.get("close") or []


def pearson_corr(x, y) -> float:
    n = len(x)
    if n != len(y) or n < 2:
        return float("nan")
    mx = sum(x) / n
    my = sum(y) / n
    sxx = 0.0
    syy = 0.0
    sxy = 0.0
    for i in range(n):
        dx = x[i] - mx
        dy = y[i] - my
        sxx += dx * dx
        syy += dy * dy
        sxy += dx * dy
    if sxx <= 0.0 or syy <= 0.0:
        return float("nan")
    return sxy / math.sqrt(sxx * syy)


def disp_code_for_mode(raw_code: str, mode: str) -> str:
    if mode == "sg":
        return raw_code.removesuffix(".SI")
    if mode == "cc":
        return raw_code.removesuffix("-USD")
    return raw_code


def _relative_luminance(rgba):
    r, g, b, _a = rgba
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _clamp(v, lo, hi):
    return lo if v < lo else hi if v > hi else v


def plot_corr_matrix(corr, labels):
    n = len(labels)

    cmap = LinearSegmentedColormap.from_list("rwb", ["red", "white", "blue"])
    norm = Normalize(vmin=-1, vmax=1)

    fig_w = _clamp(6.0 + 0.45 * n, 7.0, 11.0)
    fig_h = _clamp(5.5 + 0.42 * n, 6.5, 10.5)

    tick_fs = int(_clamp(12 - 0.55 * max(0, n - 5), 6, 12))
    ann_fs = int(_clamp(11 - 0.65 * max(0, n - 5), 5, 11))
    title_fs = int(_clamp(14 - 0.25 * max(0, n - 8), 11, 14))

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), constrained_layout=True)
    im = ax.imshow(corr, vmin=-1, vmax=1, cmap=cmap)

    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=tick_fs)
    ax.set_yticklabels(labels, fontsize=tick_fs)

    ax.set_title("Correlation Matrix", pad=18, fontsize=title_fs)

    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Correlation")
    ticks = [-1.0, -0.5, 0.0, 0.5, 1.0]
    cbar.set_ticks(ticks)
    cbar.set_ticklabels([f"{t:+.2f}" for t in ticks])

    for i in range(n):
        for j in range(n):
            v = corr[i][j]
            if not is_finite(v):
                continue
            rgba = cmap(norm(v))
            lum = _relative_luminance(rgba)
            text_color = "white" if lum < 0.55 else "black"
            ax.text(
                j,
                i,
                f"{v:+.2f}",
                ha="center",
                va="center",
                fontsize=ann_fs,
                fontweight="bold",
                color=text_color,
            )

    fig.set_constrained_layout_pads(w_pad=0.02, h_pad=0.02, wspace=0.02, hspace=0.02)
    plt.show()


def main():
    ap = argparse.ArgumentParser(
        description="Compute correlation heatmap on daily log returns over last 200 trading days (Yahoo chart API, adj close preferred)."
    )
    ap.add_argument(
        "--mode",
        choices=["sg", "us", "cc"],
        required=True,
        help=(
            "Market mode: 'sg' for SGX ('.SI' appended), 'us' for US (as-is), "
            "'cc' for crypto ('-USD' appended)."
        ),
    )
    ap.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help=(
            "Space-separated codes (e.g., D05 C6L; AAPL MSFT; BTC ETH; ^STI ^GSPC). "
            "SGX '.SI' optional, crypto '-USD' optional."
        ),
    )
    ap.add_argument(
        "--exclude",
        nargs="+",
        default=[],
        help="Space-separated codes to exclude (mode normalization applied).",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.3,
        help="Seconds to sleep between requests.",
    )
    args = ap.parse_args()

    # Handle 'auto' mode for symbols: load from all_<mode>_stocks.txt
    if args.symbols and len(args.symbols) == 1 and args.symbols[0].lower() == "auto":
        auto_file = f"all_{args.mode}_stocks.txt"
        try:
            with open(auto_file, "r", encoding="utf-8") as f:
                text = f.read()
        except FileNotFoundError:
            print(f"ERROR: Auto symbols file not found: {auto_file}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: Failed to read auto symbols file {auto_file}: {e}", file=sys.stderr)
            sys.exit(1)
        input_symbols = text.split()
        if not input_symbols:
            print(f"ERROR: Auto symbols file {auto_file} contains no symbols.", file=sys.stderr)
            sys.exit(1)
    else:
        input_symbols = args.symbols

    exclude_symbols = args.exclude or []

    if args.mode == "sg":
        exclude_norm = {ensure_si(s) for s in exclude_symbols}
        symbols_norm = [ensure_si(s) for s in input_symbols]
    elif args.mode == "cc":
        exclude_norm = {ensure_cc(s) for s in exclude_symbols}
        symbols_norm = [ensure_cc(s) for s in input_symbols]
    else:  # us
        exclude_norm = {s.strip().upper() for s in exclude_symbols}
        symbols_norm = [s.strip().upper() for s in input_symbols]

    counts = Counter(symbols_norm)
    duplicates = [f"{sym} (x{counts[sym]})" for sym in counts if counts[sym] > 1]
    if duplicates:
        print(
            "[WARN] Duplicate codes detected (will be de-duplicated): "
            + ", ".join(duplicates),
            file=sys.stderr,
        )

    # De-duplicate (first occurrence kept) + apply excludes
    symbols = [sym for sym in dict.fromkeys(symbols_norm) if sym not in exclude_norm]
    if len(symbols) < 2:
        print("ERROR: Need at least 2 symbols after exclusions.", file=sys.stderr)
        sys.exit(1)

    print("[INFO] Fetching scanning data...")
    try:
        warm_up(symbols[0])
    except Exception:
        pass

    price_maps = []
    counts_by_sym = {}

    for sym in tqdm(symbols, desc="Scanning", unit="symbol"):
        try:
            ts, series = fetch_price_series_1y(sym)
            mp = {}
            m = min(len(ts), len(series))
            for i in range(m):
                p = series[i]
                if is_finite(p):
                    mp[ts[i]] = float(p)
            counts_by_sym[sym] = len(mp)
            if len(mp) < 210:
                print(f"[WARN] {sym}: insufficient valid price points ({len(mp)})", file=sys.stderr)
            price_maps.append(mp)
        except Exception as e:
            print(f"ERROR: {sym}: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            time.sleep(args.sleep)

    common_ts = set(price_maps[0].keys())
    for mp in price_maps[1:]:
        common_ts &= set(mp.keys())

    common_ts = sorted(common_ts)
    if len(common_ts) < 201:
        details = ", ".join(
            f"{disp_code_for_mode(s, args.mode)}={counts_by_sym.get(s, 0)}" for s in symbols
        )
        print(
            f"ERROR: Not enough common trading days across all symbols. "
            f"Need >= 201 common prices, got {len(common_ts)}.\n"
            f"Per-symbol valid points: {details}",
            file=sys.stderr,
        )
        sys.exit(1)

    common_ts = common_ts[-201:]  # 201 prices -> 200 returns

    returns = []
    for mp in price_maps:
        prices = [mp[t] for t in common_ts]
        r = []
        for i in range(1, len(prices)):
            p0 = prices[i - 1]
            p1 = prices[i]
            if p0 <= 0 or p1 <= 0:
                r.append(float("nan"))
            else:
                r.append(math.log(p1 / p0))
        if any(not is_finite(x) for x in r):
            print("ERROR: Non-finite log returns encountered (likely non-positive prices).", file=sys.stderr)
            sys.exit(1)
        returns.append(r)

    n = len(symbols)
    corr = [[0.0 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        for j in range(n):
            corr[i][j] = pearson_corr(returns[i], returns[j])

    labels = [disp_code_for_mode(s, args.mode) for s in symbols]
    plot_corr_matrix(corr, labels)


if __name__ == "__main__":
    main()
