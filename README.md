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

Simulates how a stock has historically bounced from a reference price level to a TP target. Each touch of the reference price starts a new episode; the episode is a win if the intraday high reaches the TP within `--max_hold` trading days.

```bash
# Backtest NVDA from its latest close, 10% TP, 1-year window (defaults)
python scan_price_backtest.py --mode us --symbols NVDA

# Specific entry price and TP level
python scan_price_backtest.py --mode us --symbols NVDA --start_price 800 --tp_level 0.15

# Multiple symbols with per-symbol entry prices
python scan_price_backtest.py --mode us --symbols TSLA NVDA MSFT --start_price 360 190 400

# Auto-scan all SGX stocks over 3 years, top 10 by success count
python scan_price_backtest.py --mode sg --symbols auto --window 3 --sort_by succ_abs

# Auto-scan, show all results with no filters
python scan_price_backtest.py --mode sg --symbols auto --no_filters

# Crypto with 20% TP
python scan_price_backtest.py --mode cc --symbols BTC ETH --tp_level 0.2
```

Key flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--start_price` | latest close | Reference entry price(s); provide N values for N symbols |
| `--tp_level` | `0.10` | TP as a fraction of start_price (e.g. `0.10` = 10%) |
| `--window` | `1` | Lookback in years |
| `--max_hold` | `80` | Episodes where TP takes ≥ max_hold trading days are labelled TIMEOUT |
| `--min_episodes` | `2` | Minimum total episodes to include a symbol |
| `--success_thres` | `0.5` | Minimum effective success rate (0.0–1.0) to include a symbol |
| `--top_N` | `10` | Keep only the top N symbols after filtering; `0` = show all |
| `--sort_by` | `succ_abs` | `succ_pct`, `succ_abs`, or `none` |
| `--no_filters` | off | Sets min_episodes=0, success_thres=0.0, max_hold=∞, top_N=0 |

---

## scan_mr_backtest.py — BB(20,2) MR backtester

Simulates how a stock has historically behaved after the **close price touches the BB(20,2) lower band**. Each first touch starts an episode; the episode is a WIN if the intraday high reaches the TP within `--max_hold` trading days, FAIL if it doesn't, or OPEN if the episode is still live at the end of the data window.

```bash
# Backtest NVDA with default 10% TP, 20-day max hold
python scan_mr_backtest.py --mode us --symbols NVDA

# Custom TP level
python scan_mr_backtest.py --mode us --symbols AAPL MSFT NVDA --tp_level 0.08

# Crypto with 20% TP
python scan_mr_backtest.py --mode cc --symbols BTC ETH --tp_level 0.2

# Auto-scan all SGX stocks over 3 years, top 10 by win count
python scan_mr_backtest.py --mode sg --symbols auto --window 3 --sort_by succ_abs

# Auto-scan, show all results with no filters
python scan_mr_backtest.py --mode sg --symbols auto --no_filters
```

Key flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--tp_level` | `0.10` | TP as a fraction of entry_close (e.g. `0.10` = 10%) |
| `--max_hold` | `20` | Trading days before an un-hit episode is labelled FAIL |
| `--window` | `1` | Lookback in years |
| `--min_episodes` | `2` | Minimum total episodes to include a symbol |
| `--success_thres` | `0.5` | Minimum win rate (0.0–1.0) to include a symbol |
| `--top_N` | `10` | Keep only the top N symbols after filtering; `0` = show all |
| `--sort_by` | `succ_abs` | `succ_pct`, `succ_abs`, or `none` |
| `--no_filters` | off | Sets min_episodes=0, success_thres=0.0, top_N=0 |

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
