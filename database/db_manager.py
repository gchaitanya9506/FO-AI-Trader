import sqlite3
import pandas as pd
import os

DB_PATH = "database/market_data.db"
os.makedirs("database", exist_ok=True)

def get_conn():
    return sqlite3.connect(DB_PATH)

def save_df(df, table):
    conn = get_conn()
    df.to_sql(table, conn, if_exists="append", index=False)
    conn.close()

def load_table(table):
    conn = get_conn()
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    return df