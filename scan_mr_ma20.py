"""
scan_mr_ma20.py

Scan SGX, US, crypto, or index tickers on Yahoo and compute:
- LC (latest close)
- MA20   (20-day moving average)
- MA50   (50-day moving average)
- MA200  (200-day moving average)
- ΔLC%   = 100 * (LC - MA20) / MA20
- SD20   (20-day sample standard deviation of closes, ddof=1)
- Z      = (LC - MA20) / SD20
- ATR14  (simple average of TR for past 14 days)
- ATR%   = ATR14 / LC * 100

Usage example:
  python scan_mr_ma20.py --mode sg --symbols CC3 G13 N2IU C6L --delta_thres 0 --z_thres 0 --sort_by delta
  python scan_mr_ma20.py --mode us --symbols AAPL GOOG MSFT NVDA --delta_thres 0 --z_thres 0 --sort_by z
  python scan_mr_ma20.py --mode cc --symbols BTC ETH SOL --delta_thres 0 --z_thres 0 --sort_by z

Notes:
- --mode selects:
    'sg' for SGX (codes like 'D05', 'C6L'; mapped to Yahoo by appending '.SI'),
    'us' for US stocks (codes like 'AAPL', 'GOOG'; used as-is),
    'cc' for cryptocurrencies (codes like 'BTC', 'ETH'; mapped to Yahoo by appending '-USD').
- --symbols takes space-separated codes (no quotes), or 'auto' to load from all_<mode>_stocks.txt.
- --delta_thres:
    * if X <= 0, keep rows where Delta% <= X
    * if X > 0, keep rows where Delta% > X
    * or set to 'z' to use per-record rule:
        - if Z <= 0 then Delta% <= Z
        - if Z > 0 then Delta% >= Z
- --z_thres:
    * if X <= 0, keep rows where Z <= X
    * if X > 0, keep rows where Z > X.
- --sort_by controls sorting of the final table:
    * 'delta': sort by ΔLC%; if delta_thres <= 0 or not specified → increasing (most negative first),
               if delta_thres > 0 → decreasing (most positive first).
    * 'z':     sort by Z; if z_thres <= 0 or not specified → increasing (most negative first),
               if z_thres > 0 → decreasing (most positive first).
    * 'none':  no sorting; keep scan/order as processed (default).
- --reg_filter, when set, applies a long-term regime filter:
    * 'bull' keeps only rows where LC >= MA200
    * 'bear' keeps only rows where LC < MA200
- --exclude removes the specified symbols from being processed (normalization by mode is applied).
"""

from __future__ import annotations

import argparse
import sys
import time

from tqdm import tqdm

from yf_common import (
    atr_last,
    build_symbol_list,
    fetch_chart,
    fmtf,
    fmt_price,
    get_name_map,
    is_finite,
    load_auto_symbols,
    mean,
    std_sample,
    warm_up_cookies_and_crumb,
)


def ma_last(closes_valid, n):
    """Return last simple moving average value over window n (or NaN if insufficient)."""
    if len(closes_valid) < n:
        return float("nan")
    return mean(closes_valid[-n:])


def latest_non_none(arr):
    for x in reversed(arr):
        if x is not None:
            return x
    return float("nan")


def ma_stack_str(r):
    """
    Return a string like '(LC > MA20 > MA200)',
    ordering LC, MA20, MA200 by actual numeric value (descending).
    """
    lc    = r.get("LC")
    ma20  = r.get("MA20")
    ma200 = r.get("MA200")

    items = [
        ("LC", lc),
        ("MA20", ma20),
        ("MA200", ma200),
    ]
    items = [(name, val) for name, val in items if is_finite(val)]
    if len(items) < 2:
        return ""

    items_sorted = sorted(items, key=lambda x: x[1], reverse=True)
    labels = [name for name, _ in items_sorted]
    return "(" + " > ".join(labels) + ")"


def main():
    ap = argparse.ArgumentParser(
        description="Scan SGX, US, crypto, or index (Yahoo) and rank by Delta%% vs MA20."
    )
    ap.add_argument(
        "--mode",
        choices=["sg", "us", "cc"],
        required=True,
        help=(
            "Market mode: 'sg' for SGX (codes like D05, C6L; '.SI' will be appended), "
            "'us' for US stocks (codes like AAPL, GOOG; used as-is), "
            "'cc' for cryptocurrencies (codes like BTC, ETH; '-USD' will be appended)."
        ),
    )
    ap.add_argument(
        "--symbols",
        nargs="+",
        help=(
            "Space-separated stock/crypto codes "
            "(e.g., CC3 G13 for SGX; AAPL GOOG for US; BTC ETH for crypto). "
            "For SGX, '.SI' suffix is optional. For crypto, '-USD' suffix is optional."
        ),
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
        "--sort_by",
        choices=["delta", "z", "none"],
        default="none",
        help=(
            "Sort output by: 'delta' (ΔLC%%) or 'z' (Z) or 'none' (no sorting; keep scan order). "
            "For 'delta' and 'z': if threshold X <= 0 or not set → increasing (most negative first); "
            "if X > 0 → decreasing (most positive first)."
        ),
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
        help=(
            "Space-separated codes to exclude "
            "('.SI' optional for SGX; '-USD' optional for crypto; '^' optional for indexes)."
        ),
    )
    ap.add_argument(
        "--reg_filter",
        choices=["bull", "bear"],
        default=None,
        help="If set, apply long-term regime filter: 'bull' keeps LC >= MA200; 'bear' keeps LC < MA200.",
    )
    args = ap.parse_args()

    # Symbols must be provided for all modes (unless using 'auto' later).
    if not args.symbols:
        print(
            "ERROR: No symbols provided. Please supply at least one via --symbols.",
            file=sys.stderr,
        )
        return

    # Handle 'auto' mode for symbols: load from all_<mode>_stocks.txt
    if args.symbols and len(args.symbols) == 1 and args.symbols[0].lower() == "auto":
        try:
            input_symbols = load_auto_symbols(args.mode)
        except (FileNotFoundError, ValueError) as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return
    else:
        input_symbols = args.symbols

    exclude_symbols = args.exclude if args.exclude else []
    symbols_si = build_symbol_list(args.mode, input_symbols, exclude_symbols)

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols_si[0])
    except Exception:
        pass

    name_map = get_name_map(symbols_si)

    results = []

    for sym in tqdm(symbols_si, desc="Scanning", unit="symbol"):
        try:
            chart = fetch_chart(sym, "1y")
            closes = chart["close"]
            highs  = chart["high"]
            lows   = chart["low"]

            closes_valid = [c for c in closes if c is not None]
            if len(closes_valid) == 0:
                raise ValueError("No close prices in 1Y history")

            ma20  = ma_last(closes_valid, 20)
            ma50  = ma_last(closes_valid, 50)
            ma200 = ma_last(closes_valid, 200)

            rmp    = chart.get("regular_market_price")
            latest = rmp if is_finite(rmp) else latest_non_none(closes)

            sd20 = (
                std_sample(closes_valid[-20:])
                if len(closes_valid) >= 20
                else float("nan")
            )

            if is_finite(ma20) and ma20 != 0:
                delta_pct = 100.0 * (latest - ma20) / ma20
            else:
                delta_pct = float("nan")

            # Z = (LC - MA20) / SD20
            z = (
                (latest - ma20) / sd20
                if (
                    is_finite(latest)
                    and is_finite(ma20)
                    and is_finite(sd20)
                    and sd20 != 0
                )
                else float("nan")
            )

            # ATR14 (simple average of last 14 TR values)
            atr14 = atr_last(highs, lows, closes, 14)

            # ATR% = ATR14 / LC * 100
            atr_pct = (
                100.0 * atr14 / latest
                if is_finite(atr14) and is_finite(latest) and latest != 0
                else float("nan")
            )

            # Display symbol stripping suffixes/prefixes based on mode
            raw_code = sym
            if args.mode == "sg":
                disp_code = raw_code.removesuffix(".SI")
            elif args.mode == "cc":
                disp_code = raw_code.removesuffix("-USD")
            else:
                disp_code = raw_code

            results.append(
                {
                    "Symbol": disp_code,
                    "Name":   name_map.get(sym, sym),
                    "LC":     latest,
                    "MA20":   ma20,
                    "MA50":   ma50,
                    "MA200":  ma200,
                    "Delta%": delta_pct,
                    "SD20":   sd20,
                    "Z":      z,
                    "ATR14":  atr14,
                    "ATR%":   atr_pct,
                }
            )
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        finally:
            time.sleep(args.sleep)

    # Base set: drop rows where Delta% isn't computable (needs MA20)
    filtered = [r for r in results if is_finite(r.get("Delta%"))]

    applied = []

    # Regime filter
    if args.reg_filter:
        if args.reg_filter == "bull":
            filtered = [
                r
                for r in filtered
                if is_finite(r.get("LC")) and is_finite(r.get("MA200")) and r["LC"] >= r["MA200"]
            ]
            applied.append("LC >= MA200 (bull)")
        elif args.reg_filter == "bear":
            filtered = [
                r
                for r in filtered
                if is_finite(r.get("LC")) and is_finite(r.get("MA200")) and r["LC"] < r["MA200"]
            ]
            applied.append("LC < MA200 (bear)")

    # Apply optional filters
    if args.delta_thres is not None:
        # 'z' mode: per-record rule:
        #   if Z <= 0: keep Delta% <= Z
        #   if Z > 0 : keep Delta% >= Z
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
                filtered = [
                    r
                    for r in filtered
                    if is_finite(r.get("Delta%")) and r["Delta%"] <= thr
                ]
                applied.append(f"Delta% ≤ {thr:.2f}%")
            else:
                filtered = [
                    r
                    for r in filtered
                    if is_finite(r.get("Delta%")) and r["Delta%"] > thr
                ]
                applied.append(f"Delta% > {thr:.2f}%")
    if args.z_thres is not None:
        zt = float(args.z_thres)
        if zt <= 0:
            filtered = [
                r
                for r in filtered
                if is_finite(r.get("Z")) and r["Z"] <= zt
            ]
            applied.append(f"Z ≤ {zt:.2f}")
        else:
            filtered = [
                r
                for r in filtered
                if is_finite(r.get("Z")) and r["Z"] > zt
            ]
            applied.append(f"Z > {zt:.2f}")

    # ----- Sorting -----
    sort_by = args.sort_by

    if sort_by != "none":
        descending = False
        metric_key = "Delta%"  # default

        if sort_by == "delta":
            metric_key = "Delta%"
            # direction based on delta_thres (numeric only)
            if args.delta_thres is not None and not (
                isinstance(args.delta_thres, str) and args.delta_thres.lower() == "z"
            ):
                thr = float(args.delta_thres)
                if thr > 0:
                    descending = True  # most positive first
                else:
                    descending = False  # most negative first
            else:
                # no numeric threshold -> increasing (most negative first)
                descending = False
        elif sort_by == "z":
            metric_key = "Z"
            if args.z_thres is not None:
                zt = float(args.z_thres)
                if zt > 0:
                    descending = True  # most positive Z first
                else:
                    descending = False  # most negative Z first
            else:
                descending = False

        if not descending:

            def sort_key(r):
                v = r.get(metric_key)
                return (0, v) if is_finite(v) else (1, float("inf"))

        else:

            def sort_key(r):
                v = r.get(metric_key)
                return (0, -v) if is_finite(v) else (1, float("inf"))

        filtered.sort(key=sort_key)

    # ==== Summary line ====
    applied_str = "; ".join(applied) if applied else "no extra filters"
    print(
        f"\nProcessed {len(results)} valid symbols, {len(filtered)} passed filter"
        f"{'s' if len(applied) > 1 else ''}: {applied_str}\n"
    )

    # ===== One-row compact table (short labels & widths) =====
    header = (
        f"{'Code':<6} {'Name':<42} "
        f"{'LC':>6} {'MA20':>6} {'MA50':>6} {'MA200':>6} {'ΔLC%':>6} {'SD20':>6} {'Z':>5} {'ATR14':>6} {'ATR%':>5}"
    )
    print(header)
    print("-" * len(header))

    for r in filtered:
        print(
            f"{(r['Symbol'] or '')[:6]:<6} "
            f"{(r['Name'] or '')[:42]:<42} "
            f"{fmt_price(r['LC'],      6)} "
            f"{fmt_price(r['MA20'],    6)} "
            f"{fmt_price(r['MA50'],    6)} "
            f"{fmt_price(r['MA200'],   6)} "
            f"{fmtf(r['Delta%'],       6, 2)} "
            f"{fmt_price(r['SD20'],    6)} "
            f"{fmtf(r['Z'],            5, 2)} "
            f"{fmt_price(r['ATR14'],   6)} "
            f"{fmtf(r['ATR%'],         5, 2)}"
        )
        # stack = ma_stack_str(r)
        # if stack:
        #     print(stack)

    if filtered:
        print()
        print("Symbol list:")
        print(" ".join(r["Symbol"] for r in filtered))
        print()


if __name__ == "__main__":
    main()
