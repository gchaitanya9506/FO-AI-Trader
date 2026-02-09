# backtest.py
import pandas as pd
import numpy as np
import joblib
import os

def load_data(path="data/processed/nifty_option_features.csv"):
    return pd.read_csv(path)

def simple_rule_signals(df):
    # Simple rule: Buy CE if rsi < 30 and price near BB lower; Sell if rsi > 70; For options we map to calls/puts via Option Type
    # Note: df currently contains option rows; need to join with underlying indicators or compute per row.
    df = df.copy()
    # for demo assume if moneyness < 0.98 and Option Type CE -> consider buy signal
    df['signal'] = 0
    cond_call_buy = (df['Option Type'] == 'CE') & (df['moneyness'] < 1.0) & (df['implied_volatility'] < 0.5)
    cond_put_buy = (df['Option Type'] == 'PE') & (df['moneyness'] > 1.0) & (df['implied_volatility'] < 0.5)
    df.loc[cond_call_buy, 'signal'] = 1   # buy
    df.loc[cond_put_buy, 'signal'] = -1   # buy put (represented as -1)
    return df

def simulate(df):
    # naive P&L simulator using last_price percent moves
    df = df.sort_values("Date").reset_index(drop=True)
    df['last_price'] = pd.to_numeric(df.get('Last Price', 0), errors='coerce').fillna(0)
    df['next_price'] = df['last_price'].shift(-1)
    df['ret_pct'] = (df['next_price'] - df['last_price']) / (df['last_price'] + 1e-9)
    df = df.dropna(subset=['ret_pct'])
    # pnl: if signal==1 (long), pnl = ret_pct; if -1 (long put) treat same
    df['pnl'] = df['signal'] * df['ret_pct']
    summary = {
        'trades': int((df['signal'] != 0).sum()),
        'total_pnl': float(df['pnl'].sum()),
        'avg_pnl': float(df.loc[df['signal'] != 0, 'pnl'].mean()) if (df['signal'] != 0).any() else 0.0
    }
    return summary, df

if __name__ == "__main__":
    path = "data/processed/nifty_option_features.csv"
    if not os.path.exists(path):
        print("[backtest] processed data not found. Run preprocess.py first.")
    else:
        df = load_data(path)
        df_signals = simple_rule_signals(df)
        summary, df_results = simulate(df_signals)
        print("[backtest] Summary:", summary)