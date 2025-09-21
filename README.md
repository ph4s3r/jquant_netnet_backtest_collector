
# 🏹 JQUANT NETNET BACKTEST DATA COLLECTOR

**Quantitative backtest-feed tool for deep value "net-net" investing strategies**  
For each analysis date, the tool collects all available net-net candidates from J-Quants API provided financial statements and OHLC data. It does not do any actual backtest just provides the data for it.

**Analytics**:
The NCAV (Net Current Asset Value) strategy implemented here follows Benjamin Graham’s “Net-Net” approach — selecting stocks trading below their current assets minus total liabilities.
A Paper on Testing Benjamin Graham’s net current asset value model:
https://journalofeconomics.org/index.php/site/article/view/151/260

---

## 📖 Overview

High-level steps:

- Fetches a list of all TSE-listed tickers for each analysis dates
- For each analysis date it runs the following:
  - Pulls detailed financial statements and balance sheet data
  - Calculates **NCAV** and **NCAV per share (NCAVPS)**
  - Retrieves OHLC price data and computes **Margin of Safety (MoS)**
  - Identifies "net-net" stocks where  
    `share_price < NCAVPS × configurable_limit` (default: `0.8`)

The output is a set of CSV files with historical net-net candidates for each analysis date.

---

## 🚀 Technical Features / Implementation Details

- **Async I/O** powered by `asyncio` and `aiofiles` for efficient parallel requests
- **Semaphore-limited concurrency** to avoid API rate limits
- Automatic **OHLC lookback** for missing price data
- Detailed **performance logging** (tickers processed per minute, run duration, etc.)  
- Structured **CSV output** with NCAVPS, price, MoS, and disclosure dates  
- Calculates trailing 12-month dividends from /dividends endpoint (currently not used in the main process)
- Standard Logging (app/httpx/errors/tickers-where-no-ohlc-data-found)
- Performance Logging

---

## 🧠 Output

tse_netnets_analysisdate.csv

Example:

| ticker | analysis_date | ncavps | share_price | mos_rate | ncav_date  | st_disclosure_date | fs_st_skew_days |
| ------ | ------------- | ------ | ----------- | -------- | ---------- | ------------------ | --------------- |
| 17670  | 2009-12-21    | 635.12 | 450.00      | 0.70     | 2009-10-30 | 2009-10-30         |  0              |
| 18280  | 2009-12-21    | 793.93 | 600.00      | 0.75     | 2009-11-05 | 2009-10-30         |  6              |

---

## ⚙️ Configuration

- **Semaphore limit**: tune `SEMAPHORE_LIMIT` to control concurrency
- **NCAVPS threshold**: adjust `NVACPS_LIMIT` to customize Margin of Safety
- **OHLC lookback**: configurable via `OHLC_LOOKBACK_LIMIT_DAYS`
- **max_lookbehind**: lookback window for financial statements (in jquant_calc.py)

## ⚡ Concurrency & Optimal Settings

The backtester supports concurrent API calls with adjustable semaphore limits for rate control.
Through empirical testing, semaphore_limit=5 was found to be the most efficient setting — balancing throughput with J-Quants API rate limits.

## 🧪 Performance & Logging

Performance results are logged into structured CSV files, typically named like:

- performance_20250920_221744.csv – logs ticker / minute processing speed
- app_20250920_221739.log – primary application log
- httpx_20250920_221739.log – raw HTTP client logs (can grow large under load)
- errors_20250920_221739.log – contains stack traces for any unhandled exceptions

---

## 🛠️ Usage

```bash
uv run main.py
```
![alt text](image.png)