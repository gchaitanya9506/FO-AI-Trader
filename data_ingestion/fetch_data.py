# fetch_data.py
import os
from datetime import datetime, date, timedelta
import time
import requests
import pandas as pd
import yfinance as yf

# Optional libraries: nsepython or nsepy. Use whichever you have installed.
try:
    import nsepython
    HAVE_NSEPYTHON = True
except Exception as e:
    print("nsepython import error:", e)
    HAVE_NSEPYTHON = False

try:
    from nsepy import get_history
    HAVE_NSEPY = True
except Exception:
    HAVE_NSEPY = False

def fetch_nifty_data(interval="15m", ticker="^NSEI", period="60d", save_path="data/raw/nifty_data.csv"):
    print(f"[fetch_nifty_data] Fetching {ticker} interval={interval} period={period} ...")
    time.sleep(2)
    df = yf.download(ticker, interval=interval, period=period, progress=False, threads=False)
    if df is None or df.empty:
        print("[fetch_nifty_data] No data fetched. Check ticker/interval/period.")
        return None

    # normalize index -> Datetime column
    df = df.reset_index().rename(columns={"Date": "Datetime"})
    # Ensure a Datetime col with timezone-naive UTC
    df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_convert(None) if df['Datetime'].dt.tz is not None else pd.to_datetime(df['Datetime'])
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f"[fetch_nifty_data] Saved to {save_path}")
    return df

def fetch_option_chain_nse(symbol="NIFTY", save_path="data/raw/nifty_option_chain.csv"):
    print(f"[fetch_option_chain_nse] Fetching option chain for {symbol}...")

    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

    # nsepython handles cookies/headers internally
    data = nsepython.nsefetch(url)

    records = data.get("records", {}).get("data", [])
    print(f"[debug] records length: {len(records)}")

    if not records:
        print("[fetch_option_chain_nse] No rows parsed from NSE.")
        return None

    rows = []
    today = datetime.now()

    for rec in records:
        strike = rec.get("strikePrice")
        expiry = rec.get("expiryDate")

        for side in ("CE", "PE"):
            side_data = rec.get(side)
            if side_data:
                rows.append({
                    "Strike Price": strike,
                    "Option Type": side,
                    "Last Price": side_data.get("lastPrice"),
                    "IV": side_data.get("impliedVolatility"),
                    "Open Interest": side_data.get("openInterest"),
                    "Change in OI": side_data.get("changeinOpenInterest"),
                    "Date": today,
                    "Expiry": expiry
                })

    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"])
    df["Expiry"] = pd.to_datetime(df["Expiry"], errors="coerce")

    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)

    print(f"[fetch_option_chain_nse] Saved to {save_path}")
    return df

def fetch_option_chain_get_history(symbol="NIFTY", start_date=None, end_date=None, strikes=None, expiry_date = date(2026, 2, 26), save_path="data/raw/nifty_option_chain.csv"):
    """Fallback: iterate get_history for a few strikes. This can be slow and may need credentials/API access."""
    if not HAVE_NSEPY:
        print("[fetch_option_chain_get_history] nsepy not available")
        return None
    print("[fetch_option_chain_get_history] Using nsepy.get_history fallback...")
    if start_date is None:
        start_date = date.today() - timedelta(days=30)
    if end_date is None:
        end_date = date.today()
    # strikes is a list of strike prices to pull; if not provided build around ATM later
    rows = []
    if strikes is None:
        # sample strike offsets; user should refine
        strikes = [20000, 20500, 21000]  # placeholder; replace with dynamic ATM logic
    for sp in strikes:
        for opt_type in ("CE", "PE"):
            try:
                hist = get_history(symbol=symbol, start=start_date, end=end_date, option_type=opt_type, strike_price=sp, expiry_date=expiry_date)
                if hist is None or hist.empty:
                    continue
                hist = hist.reset_index()
                hist['Option Type'] = opt_type
                hist['Strike Price'] = sp
                # normalize Date column name
                if 'Date' in hist.columns:
                    hist.rename(columns={'Date': 'Date'}, inplace=True)
                rows.append(hist)
            except Exception as e:
                print(f"[fetch_option_chain_get_history] error for strike {sp} {opt_type}: {e}")
    if not rows:
        print("[fetch_option_chain_get_history] No data collected.")
        return None
    df = pd.concat(rows, ignore_index=True, sort=False)
    # ensure Date and Expiry exist as datetimes (nsepy returns date objects)
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
    if 'Expiry' in df.columns:
        df['Expiry'] = pd.to_datetime(df['Expiry'], errors='coerce')
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f"[fetch_option_chain_get_history] Saved to {save_path}")
    return df

if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    # Underlying (multiple intervals saved for convenience)
    # fetch_nifty_data(interval="15m", period="60d", save_path="data/raw/nifty_data_15m.csv")
    fetch_nifty_data(interval="5m", period="30d", save_path="data/raw/nifty_data_5m.csv")
    # fetch_nifty_data(interval="30m", period="90d", save_path="data/raw/nifty_data_30m.csv")

    # Option chain (try preferred method then fallback)
    # oc = None
    # if HAVE_NSEPYTHON:
    #     oc = fetch_option_chain_nsepython("NIFTY", save_path="data/raw/nifty_option_chain.csv")
    # if oc is None and HAVE_NSEPY:
    #     # example expiry guess; adjust dynamically as needed
    #     expiry_date = None
    #     oc = fetch_option_chain_get_history(symbol="NIFTY", expiry_date=expiry_date, save_path="data/raw/nifty_option_chain.csv")
    # if oc is None:
    #     print("[main] No option chain available. Install nsepython or nsepy or provide option CSV manually.")
    oc = fetch_option_chain_nse("NIFTY", save_path="data/raw/nifty_option_chain.csv")

    if oc is None:
        print("[main] Failed to fetch option chain.")