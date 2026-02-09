# preprocess.py
import pandas as pd
import numpy as np
import os
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import BollingerBands
from scipy.stats import norm
from datetime import datetime

def load_data(nifty_path="data/raw/nifty_data_5m.csv", option_path="data/raw/nifty_option_chain_clean.csv"):
    if not os.path.exists(nifty_path):
        raise FileNotFoundError(f"{nifty_path} not found")
    if not os.path.exists(option_path):
        raise FileNotFoundError(f"{option_path} not found")

    df_nifty = pd.read_csv(nifty_path)
    df_option = pd.read_csv(option_path)
    # normalize date columns
    if "Datetime" not in df_nifty.columns:
        # yfinance sometimes uses 'Date'
        if "Date" in df_nifty.columns:
            df_nifty = df_nifty.rename(columns={"Date": "Datetime"})
    df_nifty["Datetime"] = pd.to_datetime(df_nifty["Datetime"])
    if "Date" in df_option.columns:
        df_option["Date"] = pd.to_datetime(df_option["Date"])
    if "Expiry" in df_option.columns:
        df_option["Expiry"] = pd.to_datetime(df_option["Expiry"], errors="coerce")
    return df_nifty, df_option

def engineer_nifty_features(df_nifty):
    df = df_nifty.copy()
    df = df.sort_values("Datetime").reset_index(drop=True)
    # ensure numeric close
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    # backfill/ffill small gaps
    df["Close"] = df["Close"].ffill()
    # RSI
    df["rsi_14"] = RSIIndicator(close=df["Close"], window=14, fillna=True).rsi()
    # MACD
    macd = MACD(close=df["Close"], fillna=True)
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    # Bollinger Bands
    bb = BollingerBands(close=df["Close"], window=20, window_dev=2, fillna=True)
    df["bb_upper"] = bb.bollinger_hband()
    df["bb_lower"] = bb.bollinger_lband()
    df["bb_mid"] = bb.bollinger_mavg()
    df["bb_width"] = df["bb_upper"] - df["bb_lower"]
    # time features
    df["hour"] = df["Datetime"].dt.hour
    df["day_of_week"] = df["Datetime"].dt.dayofweek
    return df

def black_scholes_delta(option_type, S, K, T, r, sigma):
    # S: spot, K: strike, T: time in years, sigma: vol (std dev)
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    if option_type == "CE":
        return float(norm.cdf(d1))
    elif option_type == "PE":
        # delta for put: N(d1) - 1 (negative or near zero)
        return float(norm.cdf(d1) - 1.0)
    return 0.0

def engineer_option_features(df_option, df_nifty, r=0.05, default_iv=0.20, merge_tolerance_minutes=30):
    # Make copies and prepare datetimes
    df_o = df_option.copy()
    df_n = df_nifty.copy()
    df_n = df_n.sort_values("Datetime").reset_index(drop=True)
    # unify column names
    if "Date" not in df_o.columns and "Datetime" in df_o.columns:
        df_o = df_o.rename(columns={"Datetime": "Date"})
    df_o["Date"] = pd.to_datetime(df_o["Date"])
    # compute time_to_expiry in years; expiry might be date only (00:00)
    if "Expiry" in df_o.columns:
        df_o["Expiry"] = pd.to_datetime(df_o["Expiry"], errors="coerce")
        df_o["time_to_expiry"] = (df_o["Expiry"] - df_o["Date"]).dt.total_seconds() / (365 * 24 * 3600)
    else:
        df_o["time_to_expiry"] = 0.0

    # Merge the nearest previous NIFTY price using merge_asof
    # prepare keys
    df_n_for_merge = df_n[["Datetime", "Close"]].rename(columns={"Datetime": "n_datetime", "Close": "spot_price"})
    df_n_for_merge.dropna(subset=["n_datetime"], inplace=True)
    df_n_for_merge = df_n_for_merge.sort_values("n_datetime")
    df_o = df_o.sort_values("Date")
    # convert to same dtype
    df_o["Date_key"] = pd.to_datetime(df_o["Date"])
    df_n_for_merge["n_datetime"] = pd.to_datetime(df_n_for_merge["n_datetime"])
    # merge_asof expects left_on/right_on and same timezone / type
    df_merged = pd.merge_asof(df_o, df_n_for_merge, left_on="Date_key", right_on="n_datetime", direction="backward", tolerance=pd.Timedelta(minutes=merge_tolerance_minutes))
    # if no match found for some rows, spot_price will be NaN; fill with previous known or forward fill
    df_merged["spot_price"] = df_merged["spot_price"].ffill().bfill()

    # Standardize columns
    df_merged["Strike Price"] = pd.to_numeric(df_merged.get("Strike Price", df_merged.get("strikePrice", np.nan)), errors="coerce")
    df_merged["Option Type"] = df_merged.get("Option Type", df_merged.get("optionType", df_merged.get("OptionType", None)))
    # implied vol
    df_merged["IV"] = pd.to_numeric(df_merged.get("IV", df_merged.get("impliedVolatility", np.nan)), errors="coerce")
    df_merged["implied_volatility"] = df_merged["IV"] / 100.0  # if IV was percent
    # replace missing IV with default
    df_merged["implied_volatility"] = df_merged["implied_volatility"].fillna(default_iv)

    # moneyness
    df_merged["moneyness"] = df_merged["spot_price"] / df_merged["Strike Price"]

    # compute delta using black scholes helper
    df_merged["delta"] = df_merged.apply(lambda row: black_scholes_delta(
        option_type=row.get("Option Type", "CE"),
        S=float(row["spot_price"]) if pd.notnull(row["spot_price"]) else 0.0,
        K=float(row["Strike Price"]) if pd.notnull(row["Strike Price"]) else 1.0,
        T=float(row.get("time_to_expiry", 0.0)) if pd.notnull(row.get("time_to_expiry", 0.0)) else 0.0,
        r=r,
        sigma=float(row["implied_volatility"]) if pd.notnull(row["implied_volatility"]) else default_iv
    ), axis=1)

    # keep useful columns
    keep_cols = ["Date", "Expiry", "Strike Price", "Option Type", "Last Price", "spot_price", "moneyness", "IV", "implied_volatility", "time_to_expiry", "delta", "Open Interest", "Change in OI"]
    keep_present = [c for c in keep_cols if c in df_merged.columns]
    df_out = df_merged[keep_present].copy()
    return df_out

def save_processed_data(df, save_path="data/processed/nifty_option_features.csv"):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f"[save_processed_data] Saved processed data to {save_path}")

if __name__ == "__main__":
    # default file names (match fetch_data outputs)
    nifty_path = "data/raw/nifty_data_5m.csv"
    option_path = "data/raw/nifty_option_chain_clean.csv"
    nifty_df, option_df = load_data(nifty_path=nifty_path, option_path=option_path)
    print("[main] Engineering nifty features...")
    nifty_features = engineer_nifty_features(nifty_df)
    print("[main] Engineering option features...")
    option_features = engineer_option_features(option_df, nifty_features)
    save_processed_data(option_features)
    print("[preprocess] Completed.")