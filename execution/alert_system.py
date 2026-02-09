import requests
from strategy.signal_engine import generate_signal

TOKEN="8084739471:AAHlCvDxGfunAsipBYLvlP5Jpms7tFVG8pM"
CHAT_ID="946132369"

def send(msg):
    url=f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url,data={"chat_id":CHAT_ID,"text":msg})

signal = generate_signal()
send(f"AI SIGNAL: {signal}")