import joblib
from database.db_manager import load_table

_model = None

def get_model():
    global _model
    if _model is None:
        try:
            _model = joblib.load("models/saved_models/nifty_direction.model")
        except FileNotFoundError:
            print("[signal_engine] Model not found. Using fallback rules.")
            _model = None
    return _model

def generate_signal():
    model = get_model()
    if model is None:
        return "NO_MODEL_SIGNAL"

    df = load_table("underlying_features").tail(1)

    features = ["ema9","ema21","rsi","atr","vwap"]

    prob = model.predict_proba(df[features])[0][1]

    rsi = df["rsi"].values[0]

    if prob > 0.6 and rsi < 70:
        return "BUY CALL"

    if prob < 0.4 and rsi > 30:
        return "BUY PUT"

    return "NO TRADE"