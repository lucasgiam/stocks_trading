# stocks_trading

Python scripts for scanning and backtesting mean-reversion setups using Yahoo Finance daily data.

## Setup

Requires Python 3.10+.

```bash
pip install -r requirements.txt
```

No other dependencies — all network access uses the standard library (`urllib`).

## Repo structure

```
stocks_trading/
├── scan_mr_ma20.py        # MA/Z-score scanner: spot MR entry candidates
├── scan_mr_backtest.py    # BB(20,2) lower-band MR backtester
├── scan_corr.py           # Pearson correlation heatmap on daily log returns
├── scan_technicals.py     # multi-question technical scorer (15 yes/no questions)
├── yf_common.py           # shared data-fetching, math, and backtest core
├── symbols/
│   ├── all_sg_stocks.txt  # SGX tickers  (used with --symbols auto --mode sg)
│   ├── all_us_stocks.txt  # US tickers   (used with --symbols auto --mode us)
│   └── all_cc_stocks.txt  # Crypto tickers (used with --symbols auto --mode cc)
└── utils/
    ├── dedupe_symbols.py  # remove duplicates from a symbol list file
    └── extract_symbols.py # extract symbols from column A of a CSV
```

Most scripts share the same `--mode` values (see each script's `--help` for which modes it supports):

| Mode | Market | Ticker format |
|------|--------|---------------|
| `sg` | SGX | `D05`, `C6L` — `.SI` appended automatically |
| `us` | US stocks | `AAPL`, `NVDA` — used as-is |
| `cc` | Crypto | `BTC`, `ETH` — `-USD` appended automatically |

`--symbols` takes space-separated tickers, or `auto` to load from the matching `all_<mode>_stocks.txt` in `symbols/`.

## Scripts

- **scan_mr_ma20.py** — Computes LC, MA20/50/200, ΔLC%, Z-score, and ATR% per symbol to spot mean-reversion entry candidates.
  ```bash
  python scan_mr_ma20.py --mode us --symbols AAPL MSFT NVDA --z_thres -1 --sort_by z
  ```

- **scan_mr_backtest.py** — Backtests how a stock has historically behaved after its close Z-score falls to/below a threshold (default −2.0, i.e. touching the BB(20,2) lower band), simulating entry on the next open and tracking WIN/FAIL/OPEN against a TP target.
  ```bash
  python scan_mr_backtest.py --mode us --symbols NVDA --tp_level 8
  ```

- **scan_corr.py** — Plots a Pearson correlation heatmap on daily log returns over the most recent 200 trading days.
  ```bash
  python scan_corr.py --mode us --symbols AAPL MSFT NVDA TSLA
  ```

- **scan_technicals.py** — Answers 15 yes/no technical questions per symbol covering trend, momentum, volume, and MR backtest quality, then reports a total score. Supports `--mode sg`/`us`. The MR backtest questions (Q12–Q15) use the same backtesting core as `scan_mr_backtest.py` and are controlled via `--bt_*` flags.
  ```bash
  python scan_technicals.py --mode sg --symbols auto --z_thres -1 --delta_thres -4 --sort_by score --score_thres 10
  ```

Run any script with `--help` for the full list of flags and defaults.

## Symbol list files

The `symbols/all_<mode>_stocks.txt` files are whitespace-separated ticker lists consumed by `--symbols auto`. Edit them directly, then run `utils/dedupe_symbols.py` if needed.
