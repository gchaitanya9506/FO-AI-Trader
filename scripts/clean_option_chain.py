import pandas as pd
from datetime import datetime

INPUT = "data/raw/nifty_option_chain.csv"
OUTPUT = "data/raw/nifty_option_chain_clean.csv"

# Load NSE option chain (skip first header row)
df = pd.read_csv(INPUT, skiprows=1)

print("Original Columns:")
print(df.columns.tolist())

# ---------- Detect STRIKE column correctly ----------
if "STRIKE" in df.columns:
    strike_col = "STRIKE"
else:
    # Fallback: NSE strike is usually around middle
    strike_col = df.columns[10]

print("Using Strike Column:", strike_col)

# ---------- Map CE & PE columns ----------
ce_oi_col = "OI"
ce_chng_oi_col = "CHNG IN OI"
ce_iv_col = "IV"
ce_ltp_col = "LTP"

pe_ltp_col = "LTP.1"
pe_iv_col = "IV.1"
pe_chng_oi_col = "CHNG IN OI.1"
pe_oi_col = "OI.1"

rows = []
today = datetime.now()

for _, r in df.iterrows():

    # CALL ROW
    rows.append({
        "Strike Price": r.get(strike_col),
        "Option Type": "CE",
        "Last Price": r.get(ce_ltp_col),
        "IV": r.get(ce_iv_col),
        "Open Interest": r.get(ce_oi_col),
        "Change in OI": r.get(ce_chng_oi_col),
        "Date": today,
        "Expiry": today
    })

    # PUT ROW
    rows.append({
        "Strike Price": r.get(strike_col),
        "Option Type": "PE",
        "Last Price": r.get(pe_ltp_col),
        "IV": r.get(pe_iv_col),
        "Open Interest": r.get(pe_oi_col),
        "Change in OI": r.get(pe_chng_oi_col),
        "Date": today,
        "Expiry": today
    })

clean_df = pd.DataFrame(rows)

# ---------- Clean numeric columns ----------
for col in ["Strike Price","Last Price","IV","Open Interest","Change in OI"]:

    clean_df[col] = (
        clean_df[col]
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace("-", "0")
    )

    clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")

# Drop rows where strike missing
clean_df = clean_df.dropna(subset=["Strike Price"])

clean_df.to_csv(OUTPUT, index=False)

print("Cleaned option chain saved to:", OUTPUT)