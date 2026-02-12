# app.py
from flask import Flask, jsonify
import pandas as pd
import os
from config.app_config import get_config
from config.logging_config import get_logger

logger = get_logger('webapp')
config = get_config()
app = Flask(__name__)

DATA_PATH = os.path.join(config.data.processed_data_dir, "nifty_option_features.csv")

def load_latest():
    if not os.path.exists(DATA_PATH):
        return None
    df = pd.read_csv(DATA_PATH)
    # pick top few suggestions (simple rule)
    df = df.sort_values("Date", ascending=False).head(50)
    # Example suggestion: options with delta between 0.6 and 0.8 (momentum), show as buy Call
    df['suggestion'] = None
    df.loc[(df['Option Type'] == 'CE') & (df['delta'] > 0.6), 'suggestion'] = "Consider SELL CE (rich delta)"
    df.loc[(df['Option Type'] == 'CE') & (df['delta'] < 0.4), 'suggestion'] = "Consider BUY CE (cheap)"
    # return rows with suggestion
    out = df[df['suggestion'].notnull()][['Date', 'Option Type', 'Strike Price', 'Last Price', 'delta', 'implied_volatility', 'suggestion']]
    return out.to_dict(orient="records")

@app.route("/suggestions")
def suggestions():
    s = load_latest()
    if s is None:
        return jsonify({"error": "No processed data found"}), 404
    return jsonify({"suggestions": s})

if __name__ == "__main__":
    logger.info(f"Starting F&O AI Trader web application")
    logger.info(f"Environment: {config.environment}")
    logger.info(f"Debug mode: {config.debug}")
    app.run(host=config.host, port=config.port, debug=config.debug)