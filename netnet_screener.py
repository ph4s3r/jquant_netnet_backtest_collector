import yfinance as yf
import pandas as pd
import os

# ---- Beállítások ----
MAX_TICKERS = None        # None = nincs limit, az összes tickert feldolgozza
WRITE_CSV = True          # mentsünk CSV-t is az Excel mellé
DEBUG_FIRST_N = 3         # az első N tickerből írjunk ki mérleg-részletet

def load_tickers(file_path):
    """Load tickers from Excel (.xlsx) or Text (.txt), auto-detect encoding, és tegyen .T-t a JP tickerekhez."""
    ext = os.path.splitext(file_path)[1].lower()
    tickers = []
    if ext == ".xlsx":
        df = pd.read_excel(file_path)
        tickers = df.iloc[:, 0].dropna().astype(str).tolist()
        print(f"Loaded {len(tickers)} tickers from Excel")
    elif ext == ".txt":
        encodings = ["utf-8", "shift_jis", "latin-1"]
        last_error = None
        for enc in encodings:
            try:
                with open(file_path, "r", encoding=enc) as f:
                    tickers = [line.strip() for line in f if line.strip()]
                print(f"Loaded {len(tickers)} tickers with encoding {enc}")
                break
            except Exception as e:
                print(f"Encoding {enc} failed: {e}")
                last_error = e
        if not tickers:
            raise last_error or ValueError("Could not read tickers file.")
    else:
        raise ValueError("File must be .xlsx or .txt")

    # Ha a ticker csak szám (pl. '6758'), vagy nincs .T végződés, adjuk hozzá
    fixed = []
    for t in tickers:
        t = t.strip()
        if not t:
            continue
        if "." in t:
            fixed.append(t)
        else:
            fixed.append(f"{t}.T")
    return fixed

def _pick_first_row_value(bs: pd.DataFrame, candidates):
    """Keresd meg az első egyező sort (case-insensitive), és add vissza az első oszlop értékét float-ként."""
    if bs is None or bs.empty:
        return None
    idx_lower = [str(x).strip().lower() for x in bs.index]
    for cand in candidates:
        try:
            pos = idx_lower.index(cand.lower())
            val = bs.iloc[pos, 0]
            try:
                return float(val)
            except Exception:
                return None
        except ValueError:
            continue
    return None

def get_ncav(ticker, debug=False):
    """Balance sheet (annual->quarterly fallback) + market cap → NCAV & Graham-féle Net-Net mutatók."""
    try:
        stock = yf.Ticker(ticker)

        # Először éves mérleg, ha üres → negyedéves
        bs = stock.balance_sheet
        if bs is None or bs.empty:
            bs = stock.quarterly_balance_sheet

        if bs is None or bs.empty:
            if debug:
                print(f"{ticker}: balance sheet üres (annual+quarterly)")
            return None

        if debug:
            print(f"\n=== {ticker} BALANCE SHEET (top 10 rows) ===")
            try:
                print(bs.head(10))
            except Exception:
                print("(nem tudtam kiírni a fejlécet)")

        current_assets = _pick_first_row_value(bs, [
            "total current assets", "total current asset", "current assets", "current asset"
        ])
        total_liabilities = _pick_first_row_value(bs, [
            "total liabilities net minority interest",
            "total liabilities & minority interest",
            "total liabilities and minority interest",
            "total liabilities",
            "liabilities"
        ])

        if current_assets is None or total_liabilities is None:
            if debug:
                print(f"{ticker}: nem találtam megfelelő sort (Current Assets / Total Liabilities)")
            return None

        ncav = current_assets - total_liabilities

        info = stock.info or {}
        market_cap = info.get("marketCap")

        price_ncav_ratio = None
        netnet = False
        deep_netnet = False
        if (market_cap is not None) and (ncav is not None) and (ncav > 0):
            price_ncav_ratio = market_cap / ncav
            netnet = market_cap < ncav
            deep_netnet = market_cap < 0.67 * ncav

        return {
            "Ticker": ticker,
            "Current Assets": current_assets,
            "Total Liabilities": total_liabilities,
            "NCAV": ncav,
            "Market Cap": market_cap,
            "Price/NCAV": price_ncav_ratio,
            "Net-Net (<1)": netnet,
            "Deep Net-Net (<0.67)": deep_netnet
        }
    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return None

def run_netnet_screener(input_file, output_file="netnet_results.xlsx"):
    """Net-Net szűrő futtatása (összes ticker)."""
    tickers = load_tickers(input_file)
    total = len(tickers)
    print(f"Összes beolvasott ticker: {total}")

    if MAX_TICKERS:
        tickers = tickers[:MAX_TICKERS]
        print(f"Limiting to first {len(tickers)} tickers for a quick test...")

    results = []
    for i, t in enumerate(tickers, start=1):
        debug = (i <= DEBUG_FIRST_N)
        print(f"[{i}/{len(tickers)}] Processing {t}...")
        data = get_ncav(t, debug=debug)
        if data:
            results.append(data)

    if not results:
        print("No data collected.")
        return None, None, None

    df = pd.DataFrame(results)

    # Mentés
    try:
        df.to_excel(output_file, index=False)
        print(f"Excel mentve: {output_file}")
    except Exception as e:
        print(f"Excel mentés hiba: {e}")
    if WRITE_CSV:
        try:
            df.to_csv("netnet_results.csv", index=False)
            print("CSV mentve: netnet_results.csv")
        except Exception as e:
            print(f"CSV mentés hiba: {e}")

    # Kandidátok kiírása
    try:
        netnet_candidates = df[df["Net-Net (<1)"] == True]
        deep_netnet_candidates = df[df["Deep Net-Net (<0.67)"] == True]

        print("\nNet-Net Candidates (<1):")
        if not netnet_candidates.empty:
            print(netnet_candidates[["Ticker", "Market Cap", "NCAV", "Price/NCAV"]])
        else:
            print("—")

        print("\nDeep Net-Net Candidates (<0.67):")
        if not deep_netnet_candidates.empty:
            print(deep_netnet_candidates[["Ticker", "Market Cap", "NCAV", "Price/NCAV"]])
        else:
            print("—")
    except Exception as e:
        print(f"Kandidátok listázási hiba: {e}")

    return df, None, None

if __name__ == "__main__":
    input_file = "tickers.txt"
    run_netnet_screener(input_file)

