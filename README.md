# stocks_trading

Python scripts for scanning and backtesting mean-reversion setups using Yahoo Finance daily data.

## Setup

Requires Python 3.10+.

```bash
pip install tqdm matplotlib
```

No other dependencies — all network access uses the standard library (`urllib`).

## Directory

```
stocks_trading/
├── scan_mr_ma20.py        # MA/Z-score scanner: spot MR entry candidates
├── scan_price_backtest.py # Episode-based price-level backtester
├── scan_mr_backtest.py    # BB(20,2) lower-band MR backtester
├── scan_corr.py           # Pearson correlation heatmap on daily log returns
├── dedupe_symbols.py      # Utility: remove duplicates from a symbol list file
├── extract_symbols.py     # Utility: extract symbols from column A of a CSV
├── all_sg_stocks.txt      # SGX tickers (used with --symbols auto --mode sg)
├── all_us_stocks.txt      # US tickers  (used with --symbols auto --mode us)
├── all_cc_stocks.txt      # Crypto tickers (used with --symbols auto --mode cc)
└── all_id_stocks.txt      # Index tickers  (used with --symbols auto --mode id)
```

All scripts share the same `--mode` values:

| Mode | Market | Ticker format |
|------|--------|---------------|
| `sg` | SGX | `D05`, `C6L` — `.SI` appended automatically |
| `us` | US stocks | `AAPL`, `NVDA` — used as-is |
| `cc` | Crypto | `BTC`, `ETH` — `-USD` appended automatically |
| `id` | Indexes | `^STI`, `^DJI` — used as-is, `^` stripped in display |

---

## scan_mr_ma20.py — MA/Z-score scanner

Computes LC, MA20, MA200, ΔLC%, SD20, Z-score, ATR14, and ATR% for each symbol over the past year of daily data. Use it to find mean-reversion entry candidates.

```bash
# SGX — sort by Z-score (most oversold first)
python scan_mr_ma20.py --mode sg --symbols D05 C6L G13 --delta_thres 0 --z_thres 0 --sort_by z

# US — oversold screen (Z <= -1)
python scan_mr_ma20.py --mode us --symbols AAPL MSFT NVDA --z_thres -1 --sort_by z

# Auto-scan all SGX stocks, bear regime only
python scan_mr_ma20.py --mode sg --symbols auto --z_thres -1 --reg_filter bear --sort_by z

# Crypto
python scan_mr_ma20.py --mode cc --symbols BTC ETH SOL --delta_thres 0 --z_thres 0 --sort_by z

# Indexes
python scan_mr_ma20.py --mode id --symbols ^STI ^DJI ^SPX --sort_by none
```

Key flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--delta_thres X` | — | Keep rows where ΔLC% ≤ X (X ≤ 0) or ΔLC% > X (X > 0) |
| `--z_thres X` | — | Keep rows where Z ≤ X (X ≤ 0) or Z > X (X > 0) |
| `--sort_by` | `none` | `delta`, `z`, or `none` |
| `--reg_filter` | — | `bull` (LC ≥ MA200) or `bear` (LC < MA200) |

---

## scan_price_backtest.py — price-level backtester

Simulates how a stock has historically bounced from a reference price level to a TP target. Each day the low touches the reference price starts a new episode; the outcome is WIN if the intraday high reaches the TP within `--max_hold` trading days, FAIL if it doesn't, or OPEN if the data window ends before `max_hold` elapses.

```bash
# Backtest NVDA from its latest close, 10% TP, 1-year window (defaults)
python scan_price_backtest.py --mode us --symbols NVDA

# Specific entry price and TP level
python scan_price_backtest.py --mode us --symbols NVDA --price 800 --tp_level 15

# Multiple symbols with per-symbol entry prices
python scan_price_backtest.py --mode us --symbols TSLA NVDA MSFT --price 360 190 400

# Auto-scan all SGX stocks over 3 years, top 10 by win rate
python scan_price_backtest.py --mode sg --symbols auto --window 3 --sort_by succ_pct

# Auto-scan, show all results with no filters
python scan_price_backtest.py --mode sg --symbols auto --no_filters

# Crypto with 20% TP
python scan_price_backtest.py --mode cc --symbols BTC ETH --tp_level 20
```

Key flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--price` | latest close | Reference entry price(s); provide N values for N symbols |
| `--tp_level` | `10` | TP as a percentage of entry price (e.g. `10` = 10%) |
| `--window` | `1` | Lookback in years |
| `--max_hold` | `20` | TP not hit within max_hold trading days → FAIL; end of data within max_hold → OPEN |
| `--min_episodes` | `2` | Minimum total episodes to include a symbol |
| `--success_thres` | `0.5` | Minimum win rate (0.0–1.0) to include a symbol |
| `--top_N` | `10` | Keep only the top N symbols after filtering; `0` = show all |
| `--sort_by` | `succ_abs` | `succ_pct`, `succ_abs`, or `none` |
| `--no_filters` | off | Disables min_episodes, success_thres, and top_N filters |

---

## scan_mr_backtest.py — Z-score MR backtester

Simulates how a stock has historically behaved after the **close Z-score falls to or below a threshold** (default −2.0, equivalent to touching the BB(20,2) lower band). Trigger day is T; entry is the **following trading day's open** (T+1). The episode is WIN if the intraday high reaches TP within `--max_hold` trading days, FAIL if it doesn't, or OPEN if the data window ends before `max_hold` elapses.

A **reset rule** prevents chaining: after each episode, the next trigger can only fire once Z ≥ z_thres/10 has been observed since the trigger day, guarding against repeated entries inside a persistent downtrend (e.g. with the default z_thres = −2.0, reset level = −0.2).

```bash
# Backtest NVDA with default 10% TP, 20-day max hold
python scan_mr_backtest.py --mode us --symbols NVDA

# Custom TP level
python scan_mr_backtest.py --mode us --symbols AAPL MSFT NVDA --tp_level 8

# Crypto with 20% TP
python scan_mr_backtest.py --mode cc --symbols BTC ETH --tp_level 20

# Custom Z-score threshold with ΔLC% filter
python scan_mr_backtest.py --mode us --symbols NVDA --z_thres -1.5 --delta_thres -3

# Auto-scan all SGX stocks over 3 years, top 10 by win rate
python scan_mr_backtest.py --mode sg --symbols auto --window 3 --sort_by succ_pct

# Auto-scan, show all results with no filters
python scan_mr_backtest.py --mode sg --symbols auto --no_filters
```

Key flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--tp_level` | `10` | TP as a percentage of entry_price (e.g. `10` = 10%) |
| `--z_thres` | `-2.0` | Z-score trigger threshold; episode starts when Z ≤ z_thres |
| `--delta_thres` | — | Additional filter: episode only starts when ΔLC% ≤ delta_thres (ΔLC% = 100×(close−MA20)/MA20 on trigger day) |
| `--max_hold` | `20` | TP not hit within max_hold trading days → FAIL; end of data within max_hold → OPEN |
| `--window` | `1` | Lookback in years |
| `--min_episodes` | `2` | Minimum total episodes to include a symbol |
| `--success_thres` | `0.5` | Minimum win rate (0.0–1.0) to include a symbol |
| `--top_N` | `10` | Keep only the top N symbols after filtering; `0` = show all |
| `--sort_by` | `succ_abs` | `succ_pct`, `succ_abs`, or `none` |
| `--no_filters` | off | Disables min_episodes, success_thres, and top_N filters |

---

## scan_corr.py — Correlation matrix

Computes a Pearson correlation heatmap on daily log returns over the most recent 200 trading days. Uses adjusted close where available, falls back to close (common for crypto).

```bash
# SGX
python scan_corr.py --mode sg --symbols D05 C6L G13

# US
python scan_corr.py --mode us --symbols AAPL MSFT NVDA TSLA

# Auto-scan all US stocks
python scan_corr.py --mode us --symbols auto
```

---

## Utility scripts

```bash
# Remove duplicates from a symbol list (prints de-duped list to stdout)
python dedupe_symbols.py --input all_sg_stocks.txt

# Extract symbols from column A of a CSV
python extract_symbols.py --input my_watchlist.csv
```

---

## Symbol list files

The `all_<mode>_stocks.txt` files are whitespace-separated ticker lists consumed by `--symbols auto`. Edit them directly, then run `dedupe_symbols.py` if needed.
