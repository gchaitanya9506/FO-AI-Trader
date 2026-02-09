import pandas as pd
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange
from ta.volume import VolumeWeightedAveragePrice
from ta.momentum import RSIIndicator
from database.db_manager import save_df
from feature_engineering.oi_signals import compute_window_pcr

def calculate_pcr(df_option):

    ce_oi = df_option[df_option["Option Type"]=="CE"]["Open Interest"].sum()
    pe_oi = df_option[df_option["Option Type"]=="PE"]["Open Interest"].sum()

    if ce_oi == 0:
        return 0

    return pe_oi / ce_oi

def build_underlying_features(path):

    df = pd.read_csv(path)

    df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with invalid values (like the '^NSEI' row)
    df.dropna(subset=["Close", "Datetime"], inplace=True)
    df = df.sort_values("Datetime").reset_index(drop=True)

    # EMA
    df["ema9"] = EMAIndicator(df["Close"], 9).ema_indicator()
    df["ema21"] = EMAIndicator(df["Close"], 21).ema_indicator()

    # ATR
    atr = AverageTrueRange(df["High"], df["Low"], df["Close"])
    df["atr"] = atr.average_true_range()

    # VWAP
    vwap = VolumeWeightedAveragePrice(df["High"], df["Low"], df["Close"], df["Volume"])
    df["vwap"] = vwap.volume_weighted_average_price()

    # RSI
    df["rsi"] = RSIIndicator(df["Close"]).rsi()

    save_df(df, "underlying_features")
    print("Saved underlying features to DB")

def process_option_chain(option_path):

    df_option = pd.read_csv(option_path)

    spot_price = df_option["Strike Price"].median()  # TEMP (replace with real spot)

    window_pcr, atm, rng = compute_window_pcr(df_option, spot_price)

    print("ATM:", atm)
    print("Strike Window:", rng)
    print("Window PCR:", window_pcr)

    df_option["Open Interest"] = pd.to_numeric(df_option["Open Interest"], errors="coerce")

    pcr = calculate_pcr(df_option)

    print("Current PCR:", pcr)

    df_option["PCR"] = pcr

    save_df(df_option, "option_chain_features")

    print("Saved option chain features to DB")

if __name__ == "__main__":
    build_underlying_features("data/raw/nifty_data_5m.csv")

    process_option_chain("data/raw/nifty_option_chain_clean.csv")