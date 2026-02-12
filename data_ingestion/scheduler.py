import time
from fetch_data import fetch_nifty_data, fetch_option_chain_nse

while True:
    print("Fetching latest market data...")

    fetch_nifty_data(interval="5m", ticker="^NSEI",
                     save_path="data/raw/nifty_data_5m.csv")

    fetch_nifty_data(interval="5m", ticker="^BSESN",
                     save_path="data/raw/sensex_data_5m.csv")

    fetch_option_chain_nse("NIFTY")

    time.sleep(300)  # every 5 min