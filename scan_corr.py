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
import math
import sys
import time

import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, Normalize
from tqdm import tqdm

from yf_common import (
    build_symbol_list,
    fetch_chart,
    is_finite,
    load_auto_symbols,
    warm_up_cookies_and_crumb,
)


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

    tick_fs  = int(_clamp(12 - 0.55 * max(0, n - 5), 6, 12))
    ann_fs   = int(_clamp(11 - 0.65 * max(0, n - 5), 5, 11))
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
            rgba       = cmap(norm(v))
            lum        = _relative_luminance(rgba)
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
        try:
            input_symbols = load_auto_symbols(args.mode)
        except (FileNotFoundError, ValueError) as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        input_symbols = args.symbols

    symbols = build_symbol_list(args.mode, input_symbols, args.exclude or [])
    if len(symbols) < 2:
        print("ERROR: Need at least 2 symbols after exclusions.", file=sys.stderr)
        sys.exit(1)

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols[0])
    except Exception:
        pass

    price_maps     = []
    counts_by_sym  = {}

    for sym in tqdm(symbols, desc="Scanning", unit="symbol"):
        try:
            chart = fetch_chart(sym, "1y")
            ts    = chart["timestamps"]
            series = chart["close"]
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
