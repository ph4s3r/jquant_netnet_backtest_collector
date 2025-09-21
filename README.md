
# 🏹 JQUANT NETNET BACKTESTER

**Quantitative backtesting tool for deep value "net-net" investing strategies**  
Backtest data collector on Japanese equities (TSE), NCAV (Net Current Asset Value) analysis, and price-to-NCAV screening.

A Paper on Testing Benjamin Graham’s net current asset value model:
https://journalofeconomics.org/index.php/site/article/view/151/260

**Calculation Basis**:
The NCAV (Net Current Asset Value) strategy implemented here follows Benjamin Graham’s “Net-Net” approach — selecting stocks trading below their current assets minus total liabilities.
For each analysis date, the tool collects all available net-net candidates from J-Quants financial statements and runs the screen year by year.

---

## 📖 Overview

This project automates a **net-net backtest** process:

- Fetches a list of all TSE-listed tickers
- Pulls detailed financial statements and balance sheet data
- Calculates **NCAV** and **NCAV per share (NCAVPS)**
- Retrieves OHLC price data and computes **Margin of Safety (MoS)**
- Identifies "net-net" stocks where  
  `share_price < NCAVPS × configurable_limit` (default: `0.8`)

The output is a set of CSV files with historical net-net candidates for each analysis date.

---

## 🚀 Features

- **Async I/O** powered by `asyncio` and `aiofiles` for efficient parallel requests  
- **Semaphore-limited concurrency** to avoid API rate limits  
- Automatic **OHLC lookback** for missing price data  
- Detailed **performance logging** (tickers processed per minute, run duration, etc.)  
- Clean, structured **CSV output** with NCAVPS, price, MoS, and disclosure dates  
- Calculates trailing 12-month dividends from /dividends endpoint (currently not used in the main process)
- Structured Logging (Multi-File)
- Performance Testing & Output

---

## 🧠 Example Output

| ticker | analysis_date | ncavps | share_price | mos_rate | ncav_date  | st_disclosure_date |
| ------ | ------------- | ------ | ----------- | -------- | ---------- | ------------------ |
| 17670  | 2009-12-21    | 635.12 | 450.00      | 0.70     | 2009-10-30 | 2009-10-30         |
| 18280  | 2009-12-21    | 793.93 | 600.00      | 0.75     | 2009-11-05 | 2009-10-30         |

---

## ⚙️ Configuration

- **Semaphore limit**: tune `SEMAPHORE_LIMIT` to control concurrency
- **NCAVPS threshold**: adjust `NVACPS_LIMIT` to customize Margin of Safety
- **OHLC lookback**: configurable via `OHLC_LOOKBACK_LIMIT_DAYS`
- **max_lookbehind**: lookback window for financial statements

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

![alt text](image.png)