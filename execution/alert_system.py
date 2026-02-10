import requests
from strategy.signal_engine import generate_signal
from config.secrets import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

def send(msg):
    url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url,data={"chat_id":TELEGRAM_CHAT_ID,"text":msg})

signal = generate_signal()
send(f"AI SIGNAL: {signal}")