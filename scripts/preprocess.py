
import pandas as pd
import numpy as np
import os
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import BollingerBands
from scipy.stats import norm
from datetime import datetime

def load_data(nifty_path="data/raw/nifty_data.csv", option_path="data/raw/nifty_option_chain.csv"):
    df_nifty = pd.read_csv(nifty_path, parse_dates=["Datetime"])
    df_option = pd.read_csv(option_path, parse_dates=["Date"])
    return df_nifty, df_option

def engineer_nifty_features(df_nifty):
    df_nifty = df_nifty.copy()
    df_nifty.sort_values("Datetime", inplace=True)

    # RSI
    df_nifty["rsi_14"] = RSIIndicator(close=df_nifty["Close"], window=14).rsi()

    # MACD
    macd = MACD(close=df_nifty["Close"])
    df_nifty["macd"] = macd.macd()
    df_nifty["macd_signal"] = macd.macd_signal()

    # Bollinger Bands
    bb = BollingerBands(close=df_nifty["Close"], window=20, window_dev=2)
    df_nifty["bb_upper"] = bb.bollinger_hband()
    df_nifty["bb_lower"] = bb.bollinger_lband()
    df_nifty["bb_width"] = df_nifty["bb_upper"] - df_nifty["bb_lower"]

    # Time features
    df_nifty["hour"] = df_nifty["Datetime"].dt.hour
    df_nifty["day_of_week"] = df_nifty["Datetime"].dt.dayofweek

    return df_nifty

def black_scholes_delta(option_type, S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    if option_type == "CE":
        return norm.cdf(d1)
    elif option_type == "PE":
        return -norm.cdf(-d1)
    return 0

def engineer_option_features(df_option, df_nifty):
    df_option = df_option.copy()
    df_option["Date"] = pd.to_datetime(df_option["Date"])
    df_option["Expiry"] = pd.to_datetime(df_option["Expiry"])
    df_option["time_to_expiry"] = (df_option["Expiry"] - df_option["Date"]).dt.days / 365

    # Match NIFTY price at the same time
    df_option = df_option.merge(df_nifty[["Datetime", "Close"]], left_on="Date", right_on="Datetime", how="left")
    df_option.rename(columns={"Close": "spot_price"}, inplace=True)

    # Moneyness and Delta
    df_option["moneyness"] = df_option["spot_price"] / df_option["Strike Price"]
    df_option["implied_volatility"] = df_option["IV"] / 100  # assuming IV is in %
    df_option["delta"] = df_option.apply(lambda row: black_scholes_delta(
        row["Option Type"],
        row["spot_price"],
        row["Strike Price"],
        row["time_to_expiry"],
        r=0.05,
        sigma=row["implied_volatility"] if pd.notnull(row["implied_volatility"]) else 0.2
    ), axis=1)

    return df_option

def save_processed_data(df, save_path="data/processed/nifty_option_features.csv"):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f"Saved processed data to {save_path}")

if __name__ == "__main__":
    nifty_df, option_df = load_data()
    nifty_features = engineer_nifty_features(nifty_df)
    option_features = engineer_option_features(option_df, nifty_features)
    save_processed_data(option_features)
    print("Preprocessing completed.")