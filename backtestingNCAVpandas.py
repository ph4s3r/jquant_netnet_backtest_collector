#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backtesting Portfolio Graham Style
Created on Mon Sep 15 08:09:58 2025

@author: steinlachdsc
"""

##Pandas strategy
import yfinance as yf
import pandas as pd


def ensure_t_suffix(tickers):
    return [t if "." in t else f"{t}.T" for t in tickers]

def _download_dividends_dict(tickers, start, end):
    """Return dict: symbol -> dividends Series indexed by date (ex-div date) in the range."""
    divs = {}
    for t in tickers:
        try:
            s = yf.Ticker(t).dividends
            if s is None or s.empty:
                divs[t] = pd.Series(dtype="float64")
            else:
                s = s.loc[(s.index >= start) & (s.index <= end)]
                divs[t] = s
        except Exception:
            divs[t] = pd.Series(dtype="float64")
    return divs


def _benchmark_equity_curves(entry_day, end, initial_cash, fx_to_jpy=True):
    """
    Build benchmark equity curves (JPY, TR proxies):
      - SPY  (S&P 500, USD)  -> convert to JPY
      - QQQ  (Nasdaq-100, USD)-> convert to JPY
      - 1321.T (Nikkei 225 ETF, JPY)
    Returns DataFrame with columns: SPY_TR, QQQ_TR, NIKKEI_TR
    """


    def _adj_close_series(ticker, start, end):
        """Download one ticker and return a 1-D Series of adjusted Close."""
        df = yf.download(
            ticker,
            start=start,
            end=end + pd.Timedelta(days=1),
            auto_adjust=True,          # adjusted prices ~ total return proxy
            progress=False,
        )
        if df is None or len(df) == 0:
            return None

        # Ensure we end up with a SINGLE Series
        if isinstance(df, pd.Series):
            s = df
        elif isinstance(df, pd.DataFrame):
            # Prefer 'Close', else 'Adj Close', else first numeric col
            if "Close" in df.columns:
                s = df["Close"]
            elif "Adj Close" in df.columns:
                s = df["Adj Close"]
            else:
                num = df.select_dtypes(include="number")
                if num.shape[1] == 0:
                    return None
                s = num.iloc[:, 0]
        else:
            return None

        # If it’s still 2-D for some reason, squeeze again
        if hasattr(s, "ndim") and s.ndim != 1:
            s = s.squeeze()
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]

        # Coerce to numeric and drop NaNs
        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty:
            return None

        # Make sure index is DatetimeIndex
        if not isinstance(s.index, pd.DatetimeIndex):
            s.index = pd.to_datetime(s.index, errors="coerce")
            s = s.dropna()
        return s

    start_dl = entry_day - pd.Timedelta(days=7)

    spy = _adj_close_series("SPY", start_dl, end)
    qqq = _adj_close_series("QQQ", start_dl, end)
    nik = _adj_close_series("1321.T", start_dl, end)  # JPY already

    if spy is None and qqq is None and nik is None:
        raise ValueError("No benchmark data downloaded.")

    # USDJPY for converting SPY/QQQ to JPY
    fx = None
    if fx_to_jpy and (spy is not None or qqq is not None):
        fx = _adj_close_series("JPY=X", start_dl, end)  # USDJPY
        # (We align per-series when applying.)

    curves = {}

    if spy is not None:
        s = spy.loc[spy.index >= entry_day]
        if fx_to_jpy and fx is not None:
            s = s.mul(fx.reindex(s.index).ffill(), axis=0)
        if not s.empty:
            curves["SPY_TR"] = initial_cash * (s / s.iloc[0])

    if qqq is not None:
        s = qqq.loc[qqq.index >= entry_day]
        if fx_to_jpy and fx is not None:
            s = s.mul(fx.reindex(s.index).ffill(), axis=0)
        if not s.empty:
            curves["QQQ_TR"] = initial_cash * (s / s.iloc[0])

    if nik is not None:
        s = nik.loc[nik.index >= entry_day]
        if not s.empty:
            curves["NIKKEI_TR"] = initial_cash * (s / s.iloc[0])

    bench = pd.concat(curves, axis=1)
    bench = bench.loc[(bench.index >= entry_day) & (bench.index <= end)]
    return bench


def backtest_jp_targets(
    tickers,
    purchase_date: str,
    end_date: str,
    initial_cash: float = 1_000_000.0,
    target_prices: dict | None = None,
    gain_pct: float = 0.25,
    reinvest_same_day: bool = True,
    reinvest_dividends_daily: bool = True,  # NEW: auto-reinvest dividends into current holdings
):
    tickers = ensure_t_suffix(tickers)
    start = pd.to_datetime(purchase_date)
    end = pd.to_datetime(end_date)

    # Prices for trading/valuation (use Close; executions at close)
    data = yf.download(
        tickers,
        start=start - pd.Timedelta(days=7),
        end=end + pd.Timedelta(days=1),
        auto_adjust=False,
        actions=False
    )
    px = data["Close"].dropna(how="all")

    # First trading day on/after purchase date
    first_bar = px.loc[px.index >= start]
    if first_bar.empty:
        raise ValueError("No trading data on/after purchase_date")
    entry_day = first_bar.index[0]

    cash = initial_cash
    shares = {t: 0 for t in tickers}
    entry_prices = {}
    targets = {}

    # Initial buy at entry_day close (equal cash per name)
    per_name_cash = cash / len(tickers)
    for t in tickers:
        price = px.loc[entry_day, t]
        if pd.isna(price) or price <= 0:
            continue
        qty = int(per_name_cash // price)
        if qty > 0:
            shares[t] = qty
            cash -= qty * price
            entry_prices[t] = price

    # Targets: absolute dict (normalized to .T) else % over entry
    if target_prices:
        norm = {(k if "." in k else f"{k}.T"): float(v) for k, v in target_prices.items()}
        for t in tickers:
            if t in entry_prices:
                targets[t] = norm.get(t, entry_prices[t] * (1.0 + gain_pct))
    else:
        for t in tickers:
            if t in entry_prices:
                targets[t] = entry_prices[t] * (1.0 + gain_pct)

    # NEW: fetch dividend cashflows (ex-dividend dates)
    divs_dict = _download_dividends_dict(tickers, entry_day, end)

    # Run daily loop
    equity_curve = []
    for dt, row in px.loc[entry_day:].iterrows():

        # 1) Credit dividends to cash on ex-dividend dates
        dividend_happened = False
        for t in tickers:
            if shares[t] <= 0:
                continue
            s = divs_dict.get(t)
            if s is not None and not s.empty and dt in s.index:
                div = float(s.loc[dt])
                if div > 0:
                    cash += shares[t] * div
                    dividend_happened = True

        # 2) Compute portfolio value at close (after crediting dividends)
        port_val = cash + sum((shares[t] * row.get(t, float("nan"))) for t in tickers if shares[t] > 0)
        equity_curve.append({"date": dt, "equity": port_val})

        # 3) Take-profit sales
        sold_names = []
        for t in tickers:
            if shares[t] <= 0:
                continue
            price = row.get(t)
            if pd.isna(price):
                continue
            target = targets.get(t)
            if target is not None and price >= target:
                cash += shares[t] * price
                shares[t] = 0
                sold_names.append(t)

        # 4) Reinvest policy
        #    a) After sells, optionally redeploy equally into remaining
        reinvest_needed = False
        if reinvest_same_day and sold_names:
            reinvest_needed = True

        #    b) After dividends, optionally redeploy equally into remaining
        if reinvest_dividends_daily and dividend_happened:
            reinvest_needed = True

        if reinvest_needed:
            remaining = [t for t in tickers if shares[t] > 0]
            if remaining and cash > 0:
                per_cash = cash / len(remaining)
                for t in remaining:
                    price = row.get(t)
                    if pd.isna(price) or price <= 0:
                        continue
                    qty = int(per_cash // price)
                    if qty > 0:
                        shares[t] += qty
                        cash -= qty * price

    ec = pd.DataFrame(equity_curve).set_index("date")
    total_return = (ec["equity"].iloc[-1] / initial_cash) - 1.0
    years = (ec.index[-1] - ec.index[0]).days / 365.25
    cagr = (ec["equity"].iloc[-1] / initial_cash) ** (1/years) - 1 if years > 0 else float("nan")
    max_dd = ((ec["equity"] / ec["equity"].cummax()) - 1).min()

    # NEW: Benchmarks (TR proxies with adj close; SPY/QQQ converted to JPY)
    bench = _benchmark_equity_curves(entry_day, ec.index[-1], initial_cash, fx_to_jpy=True)

    return {
        "equity_curve": ec,                 # your portfolio (JPY)
        "benchmarks": bench,                # SPY_TR, QQQ_TR, NIKKEI_TR (JPY)
        "final_value": float(ec["equity"].iloc[-1]),
        "total_return": float(total_return),
        "CAGR": float(cagr),
        "max_drawdown": float(max_dd),
        "final_positions": shares,
        "cash": float(cash),
        "entry_day": entry_day,
        "end_date": ec.index[-1],
    }

if __name__ == "__main__":
    TICKERS = ["8002", "8001", "9432"]
    PURCHASE_DATE = "2023-09-15"
    END_DATE = (pd.to_datetime(PURCHASE_DATE) + pd.Timedelta(days=365*2)).date().isoformat()

    res = backtest_jp_targets(
        tickers=TICKERS,
        purchase_date=PURCHASE_DATE,
        end_date=END_DATE,
        initial_cash=2_000_000.0,
        target_prices={"8002": 3600, "8001": 9000},  # absolute targets (JPY); others use gain_pct
        gain_pct=0.25,
        reinvest_same_day=True,
        reinvest_dividends_daily=True,               # auto-reinvest dividend cash
    )

    print("Entry day:", res["entry_day"].date())
    print("End date:", res["end_date"].date())
    print("Final value:", round(res["final_value"], 2))
    print("Total return:", f'{res["total_return"]:.2%}')
    print("CAGR:", f'{res["CAGR"]:.2%}')
    print("Max DD:", f'{res["max_drawdown"]:.2%}')
    print("Final positions:", res["final_positions"])
    print("Cash:", round(res["cash"], 2))

    # Optional: simple plot
    import matplotlib.pyplot as plt
    ec = res["equity_curve"]
    bench = res["benchmarks"]

    plt.figure()
    (ec["equity"] / ec["equity"].iloc[0]).plot(label="Portfolio")
    if "SPY_TR" in bench: (bench["SPY_TR"] / bench["SPY_TR"].iloc[0]).plot(label="S&P 500 (TR, JPY)")
    if "QQQ_TR" in bench: (bench["QQQ_TR"] / bench["QQQ_TR"].iloc[0]).plot(label="Nasdaq-100 (TR, JPY)")
    if "NIKKEI_TR" in bench: (bench["NIKKEI_TR"] / bench["NIKKEI_TR"].iloc[0]).plot(label="Nikkei 225 (TR)")

    plt.title("Portfolio vs Benchmarks (normalized)")
    plt.legend()
    plt.tight_layout()
    plt.show()
    
combined = res["equity_curve"].rename(columns={"equity": "Portfolio"})
bench = res["benchmarks"]
out = combined.join(bench, how="outer")
out.to_csv("portfolio_vs_benchmarks.csv")
print("Saved: portfolio_vs_benchmarks.csv")
    