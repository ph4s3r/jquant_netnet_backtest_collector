#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backtesting Portfolio Graham Style
Created on Mon Sep 15 08:09:58 2025

@author: steinlachdsc
"""

##Pandas strategy
# jp_price_target_pandas.py
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def ensure_t_suffix(tickers):
    out = []
    for t in tickers:
        out.append(t if "." in t else f"{t}.T")
    return out

def backtest_jp_targets(
    tickers,
    purchase_date: str,
    end_date: str,
    initial_cash: float = 1_000_000.0,
    target_prices: dict | None = None,
    gain_pct: float = 0.25,
    reinvest_same_day: bool = True,
):
    tickers = ensure_t_suffix(tickers)
    start = pd.to_datetime(purchase_date)
    end = pd.to_datetime(end_date)

    data = yf.download(tickers, start=start - pd.Timedelta(days=7), end=end + pd.Timedelta(days=1))
    # use Close prices
    px = data["Close"].dropna(how="all")

    # pick first valid trading day on/after purchase_date
    first_bar = px.loc[px.index >= start]
    if first_bar.empty:
        raise ValueError("No trading data on/after purchase_date")
    entry_day = first_bar.index[0]

    cash = initial_cash
    shares = {t: 0 for t in tickers}
    entry_prices = {}
    targets = {}

    # initial buy at entry_day close
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

    # set targets
    if target_prices:
        for k, v in target_prices.items():
            kk = k if "." in k else f"{k}.T"
            targets[kk] = float(v)
    else:
        for t in tickers:
            if t in entry_prices:
                targets[t] = entry_prices[t] * (1.0 + gain_pct)

    equity_curve = []
    for dt, row in px.loc[entry_day:].iterrows():
        # compute portfolio value at close
        port_val = cash + sum((shares[t] * row.get(t, float("nan"))) for t in tickers if shares[t] > 0)
        equity_curve.append({"date": dt, "equity": port_val})

        # check take-profits
        sold_names = []
        for t in tickers:
            if shares[t] <= 0:
                continue
            price = row.get(t)
            if pd.isna(price):
                continue
            target = targets.get(t)
            if target is not None and price >= target:
                # sell all at close
                cash += shares[t] * price
                shares[t] = 0
                sold_names.append(t)

        # reinvest into remaining
        if reinvest_same_day and sold_names:
            remaining = [t for t in tickers if shares[t] > 0]
            if remaining:
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

    return {
        "equity_curve": ec,
        "final_value": float(ec["equity"].iloc[-1]),
        "total_return": float(total_return),
        "CAGR": float(cagr),
        "max_drawdown": float(max_dd),
        "final_positions": shares,
        "cash": float(cash),
        "entry_day": entry_day,
    }


if __name__ == "__main__":
    TICKERS = ["7203", "9984", "9432"]
    PURCHASE_DATE = "2023-09-15"
    END_DATE = (pd.to_datetime(PURCHASE_DATE) + pd.Timedelta(days=365*2)).date().isoformat()
    res = backtest_jp_targets(
        TICKERS,
        PURCHASE_DATE,
        END_DATE,
        initial_cash=2_000_000.0,
        target_prices= {"7203": 3000, "9984": 8000},
        gain_pct=0.25,
        reinvest_same_day=True,
    )
    print("Final value:", round(res["final_value"], 2))
    print("Total return:", f'{res["total_return"]:.2%}')
    print("CAGR:", f'{res["CAGR"]:.2%}')
    print("Max DD:", f'{res["max_drawdown"]:.2%}')
    print("Final positions:", res["final_positions"])
    print("Cash:", round(res["cash"], 2))
    print("Entry day:", res["entry_day"])
    print("End date:", END_DATE)
    
    
    
    
    
    ####### Lumibot