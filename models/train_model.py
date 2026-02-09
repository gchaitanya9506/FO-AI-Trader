import pandas as pd
import xgboost as xgb
import joblib
from database.db_manager import load_table

df = load_table("underlying_features")

df["target"] = (df["Close"].shift(-1) > df["Close"]).astype(int)

features = ["ema9","ema21","rsi","atr","vwap"]

df = df.dropna()

X = df[features]
y = df["target"]

model = xgb.XGBClassifier(n_estimators=200, max_depth=5)
model.fit(X,y)

joblib.dump(model,"models/saved_models/nifty_direction.model")

print("Model trained")