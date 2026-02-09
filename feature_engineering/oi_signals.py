import pandas as pd
import numpy as np

def get_atm_strike(df_option, spot_price, step=50):
    """
    Round spot price to nearest strike step (e.g., 50 for NIFTY)
    """
    return int(round(spot_price / step) * step)


def compute_window_pcr(df_option, spot_price, step=50, window=2):
    """
    Compute PCR using only strikes around ATM (ATM ± window*step)
    """

    # Ensure numeric columns
    df_option["Strike Price"] = pd.to_numeric(df_option["Strike Price"], errors="coerce")
    df_option["Open Interest"] = pd.to_numeric(df_option["Open Interest"], errors="coerce")

    atm = get_atm_strike(df_option, spot_price, step)

    lower = atm - (window * step)
    upper = atm + (window * step)

    df_window = df_option[
        (df_option["Strike Price"] >= lower) &
        (df_option["Strike Price"] <= upper)
    ]

    ce_oi = df_window[df_window["Option Type"] == "CE"]["Open Interest"].sum()
    pe_oi = df_window[df_window["Option Type"] == "PE"]["Open Interest"].sum()

    if ce_oi == 0:
        return 0, atm, (lower, upper)

    return pe_oi / ce_oi, atm, (lower, upper)