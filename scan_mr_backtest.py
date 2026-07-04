"""
scan_mr_backtest.py

Mean-reversion backtester driven by a Z-score threshold.  For each symbol, pulls daily
OHLCV from Yahoo Finance (lookback window configurable via --window, default 1 year) and
simulates how the stock has historically behaved after the close Z-score falls to or below
a threshold (default −2.0, equivalent to touching the BB(20,2) lower band).

Episode model:
  Trigger  : close Z-score (= (close − MA20) / SD20) <= z_thres on day T.
             The first such close in the lookback window starts episode 1.
             While an episode is active, further touches are ignored; the next
             episode search resumes only after the current episode resolves.
  Entry    : the FOLLOWING trading day (T+1).  entry_price = open on day T+1.
  TP       : entry_price * (1 + tp_level/100).  Checked via intraday high from day T+1
             onward (including the entry day itself).
  WIN      : intraday high >= tp_price within max_hold trading days.
  FAIL     : max_hold trading days elapsed without intraday high >= tp_price.
  OPEN     : end of data reached before max_hold elapsed and TP not yet hit.

Reset rule (prevents chaining episodes inside a persistent downtrend):
  After each episode, a new episode may only start once Z >= z_thres/10 has been
  seen at least once since the trigger day.  If the reset level was reached during
  the episode, the next trigger search resumes normally from i_next.  If not (e.g.
  the stock stayed deeply oversold throughout), the scan fast-forwards until
  Z >= z_thres/10 before looking for the next trigger.
  Example: z_thres = -2.0 → reset level = -0.2.

BB(20,2):
  MA20     = simple 20-day moving average of closes.
  SD20     = 20-day sample standard deviation of closes (ddof=1).
  Lower BB = MA20 − 2 × SD20.

Usage example:
  python scan_mr_backtest.py --mode sg --symbols D05 C6L --tp_level 8
  python scan_mr_backtest.py --mode us --symbols AAPL MSFT NVDA --sort_by succ_pct
  python scan_mr_backtest.py --mode us --symbols NVDA --tp_level 15
  python scan_mr_backtest.py --mode cc --symbols BTC ETH --tp_level 20
  python scan_mr_backtest.py --mode sg --symbols auto --min_episodes 3 --sort_by succ_pct
  python scan_mr_backtest.py --mode us --symbols AAPL --window 3
  python scan_mr_backtest.py --mode us --symbols NVDA --z_thres -1.5 --delta_thres -3

Output per symbol includes a summary line of the form:
  LC: XX | ΔLC%: Y% | Z: Z | Sample Success Rate: X/Y (Z%) | True Success Rate: A% to B%
  where the True Success Rate is a 95% Wilson score confidence interval for the
  underlying win probability, estimated from the closed-episode sample (wins + fails;
  OPEN episodes are excluded as inconclusive).

Notes:
- --mode selects:
    'sg' for SGX (codes like 'D05', 'C6L'; mapped to Yahoo by appending '.SI'),
    'us' for US stocks (codes like 'AAPL', 'GOOG'; used as-is),
    'cc' for cryptocurrencies (codes like 'BTC', 'ETH'; mapped to Yahoo by appending '-USD').
- --symbols takes space-separated codes (no quotes), or 'auto' to load from all_<mode>_stocks.txt.
    When explicit symbols are provided (not 'auto'), all output filters are disabled
    automatically (equivalent to --no_filters) so every symbol is always shown.
- --tp_level sets the take-profit distance as a percentage of entry_price (default: 10 = 10%).
- --z_thres sets the Z-score trigger threshold (default: -2.0).
- --delta_thres optional additional trigger filter: episode only starts when
    ΔLC% = 100*(trigger_close − MA20)/MA20 <= delta_thres.  Disabled by default.
- --max_hold caps the maximum holding period in trading days:
    * TP not hit within max_hold trading days → FAIL.
    * End of data reached before max_hold elapsed without TP → OPEN.
    * default: 20.
- --sort_by controls sorting of the final summary table (applies to --symbols auto only):
    * 'succ_pct':  sort by win rate %% (wins / total episodes), descending (default).
                   Tiebreak: succ_abs (win count), then average win-episode holding duration (shorter first).
    * 'succ_abs':  sort by absolute number of wins, descending.
                   Tiebreak: succ_pct (win rate), then average win-episode holding duration (shorter first).
    * 'none':      keep scan order.
- --min_episodes filters the output to only show symbols with at least N total episodes
    (default: 2; ignored when explicit symbols given).
- --success_thres filters to symbols whose closed-episode win rate
    (wins / (wins + fails); OPEN episodes excluded as inconclusive) >= threshold.
    Expressed as an absolute percentage (e.g. 50 = 50%%; default: 50; ignored when explicit symbols given).
- --top_N keep only the top N symbols after all filters (default: 10; 0 = show all;
    ignored when explicit symbols given).
- --no_filters disables min_episodes, success_thres, and top_N filters (useful with 'auto').
- --exclude removes the specified symbols from being processed (mode normalisation applied).
- --sleep sets the delay in seconds between Yahoo Finance requests (default: 0.5).
- --window sets the historical lookback period in years (positive integer, default: 1).
"""

from __future__ import annotations

import argparse
import math
import sys
import time

from tqdm import tqdm

from yf_common import (
    analyze_mr_bb,
    build_symbol_list,
    compute_lower_bb,
    compute_z_arr,
    fetch_chart,
    get_name_map,
    is_finite,
    load_auto_symbols,
    ts_to_date,
    warm_up_cookies_and_crumb,
)


def _auto_dp(price) -> int:
    """Decimal places based on price magnitude."""
    if not is_finite(price) or price == 0:
        return 2
    if price >= 100:
        return 2
    if price >= 10:
        return 3
    if price >= 1:
        return 4
    return 5


def _fmt_z(z) -> str:
    return f"{z:.2f}" if is_finite(z) else "N/A"


def _fmt_pct(pct) -> str:
    return f"{pct:.2f}%" if is_finite(pct) else "N/A"


def _wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """
    Wilson score 95% CI for a binomial proportion.

    Returns (lower_pct, upper_pct) as percentage values (0–100).
    Handles n == 0 by returning (0.0, 100.0).
    The Wilson interval is preferred over the normal approximation because it
    stays within [0, 1] and performs well for small samples and extreme proportions.
    """
    if n == 0:
        return (0.0, 100.0)
    p_hat = k / n
    z2 = z * z
    center = (p_hat + z2 / (2 * n)) / (1 + z2 / n)
    margin = (z / (1 + z2 / n)) * math.sqrt(p_hat * (1 - p_hat) / n + z2 / (4 * n * n))
    lower = max(0.0, center - margin) * 100
    upper = min(1.0, center + margin) * 100
    return (lower, upper)


# ─── Core analysis (imported from yf_common) ─────────────────────────────────
# analyze_mr_bb, compute_lower_bb, compute_z_arr, ts_to_date live in yf_common.py

# Silence unused-import linters for symbols re-exported for callers of this module.
_ = (analyze_mr_bb, compute_lower_bb, compute_z_arr, ts_to_date)


# ─── Terminal output ──────────────────────────────────────────────────────────

def _print_summary(
    results: list[dict],
    min_episodes: int,
    success_thres: float,
    top_n: int,
    max_hold: int = 20,
):
    total_processed = len(results)

    def _eff_rate(r: dict) -> float:
        n_win    = len(r["successes"])
        n_fail   = sum(1 for ep in r["episodes"] if ep["outcome"] == "fail")
        n_closed = n_win + n_fail
        return n_win / n_closed if n_closed else 0.0

    filtered = [
        r for r in results
        if r["n_episodes"] >= min_episodes and _eff_rate(r) >= success_thres
    ]

    if top_n > 0 and len(filtered) > top_n:
        filtered = filtered[:top_n]

    applied_str = f"episodes >= {min_episodes}, success rate >= {success_thres:.0%}, top {top_n}"
    print(
        f"\nProcessed {total_processed} valid symbols, {len(filtered)} passed filter: "
        f"{applied_str}\n"
    )

    if not filtered:
        return

    sep = "─" * 72

    for res in filtered:
        lc  = res["latest_close"]
        dp  = _auto_dp(lc)

        def p(x, _dp=dp) -> str:
            return f"{x:.{_dp}f}" if is_finite(x) else "N/A"

        code            = res.get("disp_code", res["symbol"])
        n_succ          = len(res["successes"])
        n_fail          = sum(1 for ep in res["episodes"] if ep["outcome"] == "fail")
        n_closed        = n_succ + n_fail
        pct             = n_succ / n_closed * 100 if n_closed else 0
        succ_str        = f"{n_succ}/{n_closed} ({pct:.0f}%)"
        ci_lo, ci_hi    = _wilson_ci(n_succ, n_closed)
        ci_str          = f"{ci_lo:.0f}% to {ci_hi:.0f}%"
        z_today_str     = _fmt_z(res.get("today_z", float("nan")))
        delta_today_str = _fmt_pct(res.get("today_lc_pct", float("nan")))

        print(sep)
        print(f"  {code}  ·  {res['name']}")
        print(sep)
        print(f"  LC: {p(lc)} | ΔLC%: {delta_today_str} | Z: {z_today_str} | Sample Success Rate: {succ_str} | True Success Rate: {ci_str}")
        print()

        # Most recent episode first
        show_eps = sorted(res["episodes"], key=lambda ep: ep["entry_date"], reverse=True)

        if not show_eps:
            print("  (no BB(20,2) lower-band touches in this window)")
            print()
            continue

        rows = []
        for ep in show_eps:
            outcome = ep["outcome"]
            if outcome == "win":
                exit_str = ep["first_tp_date"] or "?"
                dur      = f"{ep['tp_dur_td']} days" if ep["tp_dur_td"] is not None else "?"
                status   = "[WIN]"
            elif outcome == "fail":
                eventual_date = ep.get("eventual_tp_date")
                eventual_dur  = ep.get("eventual_tp_dur_td")
                if eventual_date:
                    # TP hit after max_hold — report actual trading days from entry
                    exit_str = eventual_date
                    dur      = f"{eventual_dur} days"
                else:
                    # TP still not hit — show trading days elapsed to end of data
                    elapsed  = ep["td_elapsed"]
                    exit_str = "open"
                    dur      = f"{elapsed} days"
                status = "[FAIL]"
            else:  # open
                elapsed  = ep["td_elapsed"]
                exit_str = "open"
                dur      = f"{elapsed} days"
                status   = "[FAIL]" if elapsed >= max_hold else "[OPEN]"

            rows.append((
                ep["entry_date"],
                exit_str,
                dur,
                _fmt_pct(ep["lc_pct"]),
                _fmt_z(ep["z"]),
                p(ep["entry_price"]),
                p(ep["tp_price"]),
                p(ep["max_high"]),
                p(ep["min_low"]),
                status,
            ))

        dur_w  = max(max(len(r[2]) for r in rows), len("Duration"))
        pct_w  = max(max(len(r[3]) for r in rows), len("ΔLC%"))
        z_w    = max(max(len(r[4]) for r in rows), len("Z"))
        ep_w   = max(max(len(r[5]) for r in rows), len("EP"))
        tp_w   = max(max(len(r[6]) for r in rows), len("TP"))
        high_w = max(max(len(r[7]) for r in rows), len("High"))
        low_w  = max(max(len(r[8]) for r in rows), len("Low"))
        stat_w = max(max(len(r[9]) for r in rows), len("Status"))

        hdr = (
            f"  {'#':>3}  {'Entry':10}  {'→ Exit':12}  {'Duration':>{dur_w}}  "
            f"{'ΔLC%':>{pct_w}}  {'Z':>{z_w}}  {'EP':>{ep_w}}  {'TP':>{tp_w}}  {'High':>{high_w}}  {'Low':>{low_w}}  Status"
        )
        rule = (
            f"  {'─'*3}  {'─'*10}  {'─'*12}  {'─'*dur_w}  "
            f"{'─'*pct_w}  {'─'*z_w}  {'─'*ep_w}  {'─'*tp_w}  {'─'*high_w}  {'─'*low_w}  {'─'*stat_w}"
        )
        print(hdr)
        print(rule)

        for k, (entry_date, exit_str, dur, pct_val, z_val, ep_val, tp_val, high_val, low_val, status) in enumerate(rows, 1):
            print(
                f"  {k:>3}  {entry_date:10}  → {exit_str:<10}  {dur:>{dur_w}}  "
                f"{pct_val:>{pct_w}}  {z_val:>{z_w}}  {ep_val:>{ep_w}}  {tp_val:>{tp_w}}  {high_val:>{high_w}}  {low_val:>{low_w}}  {status}"
            )

        print()

    print(sep)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description=(
            "Mean-reversion backtester: uses Yahoo Finance daily data to backtest "
            "how reliably a stock bounced from a BB(20,2) lower-band close touch "
            "to a TP target within a maximum holding period."
        )
    )
    ap.add_argument(
        "--mode",
        choices=["sg", "us", "cc"],
        required=True,
        help=(
            "'sg' SGX tickers (appends .SI),  'us' US stocks,  "
            "'cc' crypto (appends -USD)"
        ),
    )
    ap.add_argument(
        "--symbols",
        nargs="+",
        help=(
            "Space-separated stock/crypto/index codes, or 'auto' to load from "
            "all_<mode>_stocks.txt."
        ),
    )
    ap.add_argument(
        "--tp_level",
        type=float,
        default=10.0,
        help="Take-profit as a percentage of entry_price (default: 10 = 10%%).",
    )
    ap.add_argument(
        "--z_thres",
        type=float,
        default=-2.0,
        help=(
            "Z-score threshold for episode trigger: episode starts when the close "
            "Z-score (= (close − MA20) / SD20) <= this value "
            "(default: -2.0, equivalent to touching the BB(20,2) lower band)."
        ),
    )
    ap.add_argument(
        "--delta_thres",
        type=float,
        default=None,
        help=(
            "ΔLC%% filter for episode trigger: episode only starts when "
            "100*(trigger_close − MA20)/MA20 <= this value in addition to the Z-score condition. "
            "Disabled by default (no filter). Negative values select deeper oversold entries "
            "(e.g. --delta_thres -5 requires trigger close to be at least 5%% below MA20)."
        ),
    )
    ap.add_argument(
        "--sort_by",
        choices=["succ_pct", "succ_abs", "none"],
        default="succ_pct",
        help=(
            "Sort output: 'succ_pct' (win %% descending; tiebreak: succ_abs then avg win duration, default), "
            "'succ_abs' (win count descending; tiebreak: succ_pct then avg win duration), or 'none'."
        ),
    )
    ap.add_argument(
        "--success_thres",
        type=float,
        default=None,
        help=(
            "Minimum closed-episode win rate (wins / (wins + fails); OPEN excluded) to include a "
            "symbol, expressed as an absolute percentage (e.g. 50 = 50%%; default: 50)."
        ),
    )
    ap.add_argument(
        "--min_episodes",
        type=int,
        default=None,
        help="Minimum total episodes to include a symbol (default: 2).",
    )
    ap.add_argument(
        "--max_hold",
        type=int,
        default=20,
        help=(
            "Maximum holding period in trading days. "
            "TP not hit within max_hold days → FAIL; "
            "end of data reached within max_hold → OPEN (default: 20)."
        ),
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
        "--window",
        type=int,
        default=1,
        help="Lookback window in years (default: 1).",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds to sleep between requests (default: 0.5).",
    )
    ap.add_argument(
        "--top_N",
        type=int,
        default=None,
        help="Keep only the top N symbols after all filters (default: 10; 0 = show all).",
    )
    ap.add_argument(
        "--no_filters",
        action="store_true",
        help=(
            "Disable all default output filters: sets min_episodes=0, "
            "success_thres=0.0, and top_N=0."
        ),
    )
    args = ap.parse_args()

    if args.window < 1:
        ap.error("--window must be a positive integer (years).")
    args.window   = f"{args.window}y"
    args.tp_level = args.tp_level / 100.0

    is_auto = (
        args.symbols
        and len(args.symbols) == 1
        and args.symbols[0].lower() == "auto"
    )

    # When symbols are explicitly provided, disable filters that weren't explicitly set.
    if not is_auto or args.no_filters:
        if args.min_episodes  is None: args.min_episodes  = 0
        if args.success_thres is None: args.success_thres = 0.0
        if args.top_N         is None: args.top_N         = 0
    # Apply true defaults for auto mode (or when filters were explicitly passed).
    if args.min_episodes  is None: args.min_episodes  = 2
    if args.success_thres is None: args.success_thres = 50.0
    if args.top_N         is None: args.top_N         = 10
    args.success_thres /= 100.0  # convert absolute percent input to fraction

    if not args.symbols:
        print("ERROR: No symbols provided. Please supply at least one via --symbols.", file=sys.stderr)
        return

    # Resolve 'auto' from file
    if is_auto:
        try:
            input_symbols = load_auto_symbols(args.mode)
        except (FileNotFoundError, ValueError) as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return
    else:
        input_symbols = args.symbols

    exclude_symbols = args.exclude if args.exclude else []
    symbols_list = build_symbol_list(args.mode, input_symbols, exclude_symbols)

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols_list[0])
    except Exception:
        pass

    name_map = get_name_map(symbols_list)

    results = []
    for sym in tqdm(symbols_list, desc="Scanning", unit="symbol"):
        try:
            chart = fetch_chart(sym, args.window)
            res   = analyze_mr_bb(sym, name_map.get(sym, sym), chart, args.tp_level, args.max_hold, args.z_thres, args.delta_thres)

            res["disp_code"] = sym.removesuffix(".SI") if args.mode == "sg" else (
                sym.removesuffix("-USD") if args.mode == "cc" else sym
            )

            results.append(res)
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        finally:
            time.sleep(args.sleep)

    if args.sort_by in ("succ_pct", "succ_abs"):
        def _avg_win_dur(r):
            """Average tp_dur_td of WIN episodes; inf if no wins (sorts last)."""
            durs = [ep["tp_dur_td"] for ep in r["successes"] if ep.get("tp_dur_td") is not None]
            return sum(durs) / len(durs) if durs else float("inf")

        def _closed_rate(r):
            n_win = len(r["successes"])
            n_fail = sum(1 for ep in r["episodes"] if ep["outcome"] == "fail")
            n_closed = n_win + n_fail
            return n_win / n_closed if n_closed else 0.0

        if args.sort_by == "succ_pct":
            results.sort(
                key=lambda r: (
                    _closed_rate(r),
                    len(r["successes"]),
                    -_avg_win_dur(r),
                ),
                reverse=True,
            )
        else:  # succ_abs
            results.sort(
                key=lambda r: (
                    len(r["successes"]),
                    _closed_rate(r),
                    -_avg_win_dur(r),
                ),
                reverse=True,
            )

    _print_summary(results, args.min_episodes, args.success_thres, args.top_N, args.max_hold)


if __name__ == "__main__":
    main()
