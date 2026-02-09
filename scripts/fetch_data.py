# Script to fetch historical data
import yfinance as yf
import pandas as pd
from nsepy.derivatives import get_expiry_date
from nsepy import get_history
from nsepython import nse_quote_ltp
from datetime import datetime, date, timedelta
import os

def fetch_nifty_data(interval, ticker="^NSEI", period="60d", save_path="data/raw/nifty_data.csv"):
    print(f"Fetching {ticker} data...")
    df = yf.download(ticker, interval=interval, period=period)
    
    if df.empty:
        print("No data fetched. Please check the ticker or parameters.")
        return

    df.reset_index(inplace=True)
    df.to_csv(save_path, index=False)
    print(f"Data saved to {save_path}")

def fetch_option_chain(symbol="NIFTY", expiry_date=None, strike_range=50, save_path="data/raw/nifty_option_chain.csv"):
    print(f"Fetching Option Chain for {symbol}...")

    if not expiry_date:
        # date = datetime.now()
        # expiry_date = get_expiry_date(date.year, date.month)[0]  # Get the latest expiry date
        expiry_date = date(2025, 4, 24)

    print(f"Using expiry date: {expiry_date}")
    
    # Fetch the option chain for Calls and Puts
    option_chain = pd.DataFrame()

    for i in range(1, 6):  # Loop for 5 strikes on both sides
        call_data = get_history(symbol=symbol, start=date(2025,4,1), end=date.today(),index=True,
                                option_type="CE", strike_price=23800 + strike_range * i, expiry_date=expiry_date)
        put_data = get_history(symbol=symbol, start=date(2025,4,1), end=date.today(),index=True,
                               option_type="PE", strike_price=23850 - strike_range * i, expiry_date=expiry_date)
        
        option_chain = pd.concat([option_chain, call_data, put_data])

    option_chain.reset_index(inplace=True)
    option_chain.to_csv(save_path, index=False)
    print(f"Option Chain data saved to {save_path}")

if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    fetch_nifty_data("15m")
    fetch_nifty_data("5m")
    fetch_nifty_data("30m")
    fetch_option_chain()