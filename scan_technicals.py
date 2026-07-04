"""
scan_technicals.py

Scan SGX or US tickers on Yahoo and answer 15 yes/no (1/0) technical
questions per symbol, then report a total score per symbol.

Data is pulled over a 2-year window since several questions need a 200-day
SMA computed up to 20 trading days back (~220 days of history required).
Q12-Q15 require a mean-reversion backtest (see --bt_* args below).

Usage examples:
  python scan_technicals.py --mode sg --symbols D05 C6L --sort_by score
  python scan_technicals.py --mode us --symbols AAPL MSFT NVDA --sort_by score delta
  python scan_technicals.py --mode us --symbols AAPL MSFT --delta_thres 0 --z_thres 0 --sort_by delta
  python scan_technicals.py --mode sg --symbols auto --z_thres -1 --delta_thres -4 --sort_by score --score_thres 10

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
  computed the same way as in scan_mr_ma20.py.
- --delta_thres / --z_thres / --sort_by: same semantics as scan_mr_ma20.py.
- MR backtest args (Q12-Q15):
    --bt_z_thres    : Z-score trigger threshold. Defaults to --z_thres if provided,
                      otherwise -2.0. Set independently to override.
    --bt_delta_thres: ΔLC% trigger threshold. Defaults to --delta_thres (numeric) if
                      provided, otherwise disabled. If only one of z/delta is given,
                      the backtest uses that filter alone.
    --bt_tp_level   : TP target as % of entry price (default 10.0).
    --bt_max_hold   : maximum hold duration in trading days (default 50).
    --bt_success_thres: minimum win rate % required to pass Q12 (default 60.0).
    --bt_window     : years of price history for the backtest (default 2).
"""

from __future__ import annotations

import argparse
import sys
import time

from tqdm import tqdm

from yf_common import (
    analyze_mr_bb,
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

# ---------- Q8 swing-low pivot width (bars required on each side) ----------
SWING_LEFT_BARS  = 2
SWING_RIGHT_BARS = 2

# ---------- short column headers ("Q1".."Q15") ----------
QUESTION_LABELS = {n: f"Q{n}" for n in range(1, 16)}

# ---------- full wording of each question (printed above the table) ----------
QUESTION_TEXT = {
    1:  "Relevant broad market index is above its 200-day SMA.",
    2:  "Relevant broad market index's 200-day SMA is higher than it was 20 days ago.",
    3:  "The stock's current price is above its 200-day SMA.",
    4:  "The stock's 200-day SMA is higher than it was 20 days ago.",
    5:  "The stock's 50-day SMA is above its 200-day SMA.",
    6:  "The stock's daily closes were above its 200-day SMA in at least 15 out of the past 20 days.",
    7:  "The stock's current price is above its lowest daily close in the past 20 days.",
    8:  "The stock's current price is above the most recent swing low close in the past 50 days.",
    9:  "The stock has 5 or less high-volume down days in the past 20 days, where high-volume means volume above the 20-day average.",
    10: "The stock's total down-day volume does not exceed its total up-day volume by more than 50% in the past 20 days.",
    11: "The stock's most recent daily close was in the upper half of its high-low range.",
    12: "MRB(2Y,50D,10%): The stock has a total of at least 8 past win/fail episodes.",
    13: "MRB(2Y,50D,10%): The stock has a total of at least 4 past win/fail episodes and at least 70% overall success rate.",
    14: "MRB(2Y,50D,10%): The stock has a total of at least 4 past win/fail episodes and across all past win episodes, the average holding duration from entry to TP is 20 days or less. Score 0 if there are no past win episodes.",
    15: "MRB(2Y,50D,10%): The stock has a total of at least 4 past win/fail episodes and across all past failed episodes, the average holding duration from entry to TP, or to the latest available date if TP has not been reached, is 100 days or less. Score 1 if there are no past failed episodes.",
}


def sma(values, n, end):
    """Simple moving average over the n values ending right before index `end`."""
    if end < n:
        return float("nan")
    window = values[end - n:end]
    return mean(window)


def find_swing_low_indices(closes, left=2, right=2):
    """Indices i where closes[i] is strictly lower than the `left` closes
    immediately before it and the `right` closes immediately after it."""
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
    highs  = chart["high"]
    lows   = chart["low"]
    vols   = chart["volume"]
    rows = []
    for i, c in enumerate(closes):
        if c is None:
            continue
        rows.append(
            {
                "close":  c,
                "high":   highs[i] if i < len(highs) else None,
                "low":    lows[i]  if i < len(lows)  else None,
                "volume": vols[i]  if i < len(vols)  else None,
            }
        )
    return rows


def compute_struct(chart):
    """Derive all per-instrument metrics needed across the 15 questions."""
    rows   = build_rows(chart)
    closes = [r["close"] for r in rows]
    highs  = [r["high"] if is_finite(r["high"]) else float("nan") for r in rows]
    lows   = [r["low"]  if is_finite(r["low"])  else float("nan") for r in rows]
    n      = len(closes)

    rmp   = chart.get("regular_market_price")
    price = rmp if is_finite(rmp) else (closes[-1] if closes else float("nan"))

    sma20_now    = sma(closes, 20,  n)
    sma200_now   = sma(closes, 200, n)
    sma200_20ago = sma(closes, 200, n - 20) if n - 20 >= 0 else float("nan")
    sma50_now    = sma(closes, 50,  n)

    sd20 = std_sample(closes[-20:]) if n >= 20 else float("nan")

    delta_pct = (
        100.0 * (price - sma20_now) / sma20_now
        if is_finite(sma20_now) and sma20_now != 0
        else float("nan")
    )

    z = (
        (price - sma20_now) / sd20
        if is_finite(price) and is_finite(sma20_now) and is_finite(sd20) and sd20 != 0
        else float("nan")
    )

    atr14     = atr_last(highs, lows, closes, 14)
    atr14_pct = (
        100.0 * atr14 / price
        if is_finite(atr14) and is_finite(price) and price != 0
        else float("nan")
    )

    return {
        "rows":         rows,
        "closes":       closes,
        "highs":        highs,
        "lows":         lows,
        "price":        price,
        "sma20_now":    sma20_now,
        "sma200_now":   sma200_now,
        "sma200_20ago": sma200_20ago,
        "sma50_now":    sma50_now,
        "sd20":         sd20,
        "delta_pct":    delta_pct,
        "z":            z,
        "atr14_pct":    atr14_pct,
    }


def evaluate_questions(stock, idx, mr_result=None, bt_success_thres=0.60):
    q = {}

    # Q1-Q2: broad market index checks
    q[1] = is_finite(idx["price"]) and is_finite(idx["sma200_now"]) and idx["price"] > idx["sma200_now"]
    q[2] = is_finite(idx["sma200_now"]) and is_finite(idx["sma200_20ago"]) and idx["sma200_now"] > idx["sma200_20ago"]

    # Q3-Q5: stock SMA structure
    q[3] = is_finite(stock["price"]) and is_finite(stock["sma200_now"]) and stock["price"] > stock["sma200_now"]
    q[4] = is_finite(stock["sma200_now"]) and is_finite(stock["sma200_20ago"]) and stock["sma200_now"] > stock["sma200_20ago"]
    q[5] = is_finite(stock["sma50_now"]) and is_finite(stock["sma200_now"]) and stock["sma50_now"] > stock["sma200_now"]

    closes = stock["closes"]
    rows   = stock["rows"]
    price  = stock["price"]
    n      = len(closes)

    # Q6: closes above their per-day SMA200 in at least 15 of the past 20 days
    if n >= 220:
        count_above = 0
        for i in range(n - 20, n):
            sma200_i = mean(closes[i - 199:i + 1])
            if is_finite(closes[i]) and is_finite(sma200_i) and closes[i] > sma200_i:
                count_above += 1
        q[6] = count_above >= 15
    else:
        q[6] = False

    # Q7: price above lowest close in the past 20 days
    if n >= 20:
        last20 = [c for c in closes[-20:] if is_finite(c)]
        low20  = min(last20) if last20 else float("nan")
        q[7]   = is_finite(price) and is_finite(low20) and price > low20
    else:
        q[7] = False

    # Q8: price above most recent confirmed swing-low close in the past 50 days
    if n >= 50 + SWING_RIGHT_BARS:
        swing_idxs    = find_swing_low_indices(closes, left=SWING_LEFT_BARS, right=SWING_RIGHT_BARS)
        recent_swings = [i for i in swing_idxs if i >= n - 50]
        if recent_swings:
            swing_low_close = closes[max(recent_swings)]
            q[8] = is_finite(price) and is_finite(swing_low_close) and price > swing_low_close
        else:
            q[8] = False
    else:
        q[8] = False

    # Q9: 5 or fewer high-volume down days in the past 20 days
    if n >= 21:
        idxs20    = range(n - 20, n)
        vols20    = [rows[i]["volume"] for i in idxs20 if is_finite(rows[i]["volume"])]
        avg_vol20 = mean(vols20) if vols20 else float("nan")
        hv_down   = 0
        for i in idxs20:
            prev_c, cur_c, vol = rows[i - 1]["close"], rows[i]["close"], rows[i]["volume"]
            if not (is_finite(prev_c) and is_finite(cur_c) and is_finite(vol) and is_finite(avg_vol20)):
                continue
            if vol > avg_vol20 and cur_c < prev_c:
                hv_down += 1
        q[9] = hv_down <= 5
    else:
        q[9] = False

    # Q10: down-day volume doesn't exceed up-day volume by more than 50% in the past 20 days
    if n >= 21:
        up_vol = down_vol = 0.0
        any_valid = False
        for i in range(n - 20, n):
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

    # Q11: most recent close in the upper half of its high-low range
    n_rows = len(rows)
    if n_rows >= 1:
        r  = rows[n_rows - 1]
        h, lo, c = r["high"], r["low"], r["close"]
        q[11] = (
            is_finite(h) and is_finite(lo) and is_finite(c)
            and h > lo and c >= (h + lo) / 2.0
        )
    else:
        q[11] = False

    # Q12-Q15: MR backtest metrics (require a valid mr_result)
    if mr_result is not None:
        episodes = mr_result["episodes"]
        closed   = [ep for ep in episodes if ep["outcome"] in ("win", "fail")]
        n_closed = len(closed)
        n_wins   = sum(1 for ep in closed if ep["outcome"] == "win")

        # Q12: at least 8 closed episodes
        q[12] = n_closed >= 8

        # Q13: at least 4 closed episodes AND win rate >= bt_success_thres
        q[13] = n_closed >= 4 and (n_wins / n_closed) >= bt_success_thres

        # Q14: at least 4 closed episodes AND average TP-hit duration <= 20 days (WIN episodes)
        #   Score 0 if no win episodes exist
        tp_durs = [
            ep["tp_dur_td"] for ep in closed
            if ep["outcome"] == "win" and ep["tp_dur_td"] is not None
        ]
        q[14] = n_closed >= 4 and bool(tp_durs) and (sum(tp_durs) / len(tp_durs)) <= 20

        # Q15: at least 4 closed episodes AND average FAIL duration <= 100 days
        #   duration = eventual_tp_dur_td if TP was eventually hit, else td_elapsed
        #   Score 1 if no fail episodes exist
        fail_durs = []
        for ep in closed:
            if ep["outcome"] != "fail":
                continue
            dur = ep["eventual_tp_dur_td"] if ep["eventual_tp_dur_td"] is not None else ep["td_elapsed"]
            if dur is not None:
                fail_durs.append(dur)
        q[15] = n_closed >= 4 and ((not fail_durs) or (sum(fail_durs) / len(fail_durs)) <= 100)
    else:
        q[12] = q[13] = q[14] = q[15] = False

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
            "score first, then ΔLC%% to break ties. "
            "'delta': sort by ΔLC%%; 'z': sort by Z; 'none': keep scan order."
        ),
    )
    ap.add_argument(
        "--score_thres",
        type=int,
        default=0,
        help="Minimum score required for a symbol to be shown (default 0, no filtering).",
    )
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
        help="Z filter: if X <= 0, keep rows where Z ≤ X; if X > 0, keep rows where Z > X.",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.3,
        help="Seconds to sleep between requests.",
    )
    ap.add_argument(
        "--bt_z_thres",
        type=float,
        default=None,
        help=(
            "MR backtest Z-score trigger threshold. "
            "Defaults to --z_thres if provided, otherwise -2.0."
        ),
    )
    ap.add_argument(
        "--bt_delta_thres",
        type=float,
        default=None,
        help=(
            "MR backtest ΔLC%% trigger threshold. "
            "Defaults to --delta_thres (numeric) if provided, otherwise no delta filter."
        ),
    )
    ap.add_argument(
        "--bt_tp_level",
        type=float,
        default=10.0,
        help="MR backtest TP target as a percentage of entry price (default 10.0, i.e. 10%%).",
    )
    ap.add_argument(
        "--bt_max_hold",
        type=int,
        default=50,
        help="MR backtest maximum hold duration in trading days (default 50).",
    )
    ap.add_argument(
        "--bt_success_thres",
        type=float,
        default=70.0,
        help="Minimum win rate %% required to pass Q13 (default 70.0).",
    )
    ap.add_argument(
        "--bt_window",
        type=int,
        default=2,
        help="Years of price history to use for the MR backtest (default 2; minimum 2 enforced for technicals).",
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

    bt_tp_level_frac    = args.bt_tp_level / 100.0
    bt_success_thres_frac = args.bt_success_thres / 100.0
    broad_index_symbol  = "^STI" if args.mode == "sg" else "^GSPC"
    chart_window        = f"{max(2, args.bt_window)}y"

    # Effective backtest trigger thresholds:
    #   bt_z_thres    follows --z_thres if not explicitly set; falls back to -2.0
    #   bt_delta_thres follows --delta_thres (numeric) if not explicitly set; None = disabled
    if args.bt_z_thres is not None:
        eff_bt_z_thres = args.bt_z_thres
    elif args.z_thres is not None:
        eff_bt_z_thres = args.z_thres
    else:
        eff_bt_z_thres = None  # resolved below

    delta_thres_numeric = (
        float(args.delta_thres)
        if args.delta_thres is not None
        and not (isinstance(args.delta_thres, str) and args.delta_thres.lower() == "z")
        else None
    )
    if args.bt_delta_thres is not None:
        eff_bt_delta_thres = args.bt_delta_thres
    elif delta_thres_numeric is not None:
        eff_bt_delta_thres = delta_thres_numeric
    else:
        eff_bt_delta_thres = None

    # If only delta filter is active (no z), pass inf so the z check in analyze_mr_bb is skipped.
    # If neither is active, fall back to default z trigger of -2.0.
    if eff_bt_z_thres is not None:
        bt_z_for_backtest = eff_bt_z_thres
    elif eff_bt_delta_thres is not None:
        bt_z_for_backtest = float("inf")   # delta-only mode: z check never triggers a skip
    else:
        bt_z_for_backtest = -2.0           # default when no threshold provided at all

    if len(args.symbols) == 1 and args.symbols[0].lower() == "auto":
        try:
            input_symbols = load_auto_symbols(args.mode)
        except (FileNotFoundError, ValueError) as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return
    else:
        input_symbols = args.symbols

    exclude_symbols = args.exclude or []
    symbols = build_symbol_list(args.mode, input_symbols, exclude_symbols)
    if not symbols:
        print("ERROR: No symbols left to scan after exclusions.", file=sys.stderr)
        return

    print("[INFO] Fetching scanning data...")
    try:
        warm_up_cookies_and_crumb(symbols[0])
    except Exception:
        pass

    try:
        idx_chart  = fetch_chart(broad_index_symbol, chart_window)
        idx_struct = compute_struct(idx_chart)
    except Exception as e:
        print(f"ERROR: Failed to fetch broad market index {broad_index_symbol}: {e}", file=sys.stderr)
        return

    name_map = get_name_map(symbols)

    results = []
    for sym in tqdm(symbols, desc="Scanning", unit="symbol"):
        try:
            chart        = fetch_chart(sym, chart_window)
            stock_struct = compute_struct(chart)
            name         = name_map.get(sym, sym)

            try:
                mr_result = analyze_mr_bb(
                    sym, name, chart,
                    bt_tp_level_frac,
                    args.bt_max_hold,
                    z_thres=bt_z_for_backtest,
                    delta_thres=eff_bt_delta_thres,
                )
            except Exception:
                mr_result = None

            q         = evaluate_questions(stock_struct, idx_struct, mr_result, bt_success_thres_frac)
            score     = sum(1 for v in q.values() if v)
            disp_code = sym.removesuffix(".SI") if args.mode == "sg" else sym
            results.append(
                {
                    "Symbol": disp_code,
                    "Name":   name,
                    "Q":      q,
                    "Score":  score,
                    "LC":     stock_struct["price"],
                    "MA20":   stock_struct["sma20_now"],
                    "MA50":   stock_struct["sma50_now"],
                    "MA200":  stock_struct["sma200_now"],
                    "Delta%": stock_struct["delta_pct"],
                    "Z":      stock_struct["z"],
                    "ATR14%": stock_struct["atr14_pct"],
                }
            )
        except Exception as e:
            print(f"[WARN] {sym}: {e}", file=sys.stderr)
        finally:
            time.sleep(args.sleep)

    qs        = list(range(1, 16))
    max_score = len(qs)

    filtered = [r for r in results if r["Score"] >= args.score_thres]

    applied = []

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

        filtered.sort(key=secondary_key)
        filtered.sort(key=lambda r: r["Score"], reverse=True)
    elif sort_mode in ("delta", "z"):
        metric_key = "Delta%" if sort_mode == "delta" else "Z"
        thr_arg    = args.delta_thres if sort_mode == "delta" else args.z_thres
        descending = False
        if thr_arg is not None and not (isinstance(thr_arg, str) and thr_arg.lower() == "z"):
            if float(thr_arg) > 0:
                descending = True

        if not descending:
            def sort_key(r):
                v = r.get(metric_key)
                return (0, v) if is_finite(v) else (1, float("inf"))
        else:
            def sort_key(r):
                v = r.get(metric_key)
                return (0, -v) if is_finite(v) else (1, float("inf"))

        filtered.sort(key=sort_key)

    bt_z_disp    = f"{bt_z_for_backtest:.2f}" if bt_z_for_backtest != float("inf") else "off"
    bt_delta_disp = f"{eff_bt_delta_thres:.2f}%" if eff_bt_delta_thres is not None else "off"
    print(
        f"\nMode={args.mode} | Broad index={broad_index_symbol} | window={chart_window} | "
        f"BT: z_thres={bt_z_disp}, delta_thres={bt_delta_disp}, "
        f"tp={args.bt_tp_level}%, max_hold={args.bt_max_hold}d, "
        f"success_thres={args.bt_success_thres:.0f}%"
    )
    applied_str = "; ".join(applied) if applied else "no extra filters"
    print(
        f"Scored {len(results)} symbols out of {max_score} applicable questions, "
        f"{len(filtered)} passed score_thres >= {args.score_thres}"
        f"{' and ' + applied_str if applied else ''}.\n"
    )

    for qn in qs:
        print(f"{QUESTION_LABELS[qn]}: {QUESTION_TEXT[qn]}")
    print()

    q_headers = " ".join(f"{QUESTION_LABELS[qn]:>3}" for qn in qs)
    header = (
        f"{'Code':<6} {'Name':<32} {q_headers} {'Score':>6} "
        f"{'LC':>6} {'MA20':>6} {'MA50':>6} {'MA200':>6} {'ΔLC%':>6} {'Z':>5} {'ATR%':>5}"
    )
    print(header)
    print("-" * len(header))

    for r in filtered:
        q_cells = " ".join(f"{1 if r['Q'][qn] else 0:>3}" for qn in qs)
        print(
            f"{(r['Symbol'] or '')[:6]:<6} "
            f"{(r['Name'] or '')[:32]:<32} "
            f"{q_cells} "
            f"{r['Score']:>3}/{max_score} "
            f"{fmt_price(r['LC'],    6)} "
            f"{fmt_price(r['MA20'],  6)} "
            f"{fmt_price(r['MA50'],  6)} "
            f"{fmt_price(r['MA200'], 6)} "
            f"{fmtf(r['Delta%'], 6, 2)} "
            f"{fmtf(r['Z'],      5, 2)} "
            f"{fmtf(r['ATR14%'], 5, 2)}"
        )

    if filtered:
        print()
        print("Symbol list:")
        print(" ".join(r["Symbol"] for r in filtered))
        print()


if __name__ == "__main__":
    main()
