def position_size(capital, risk_per_trade=0.02):
    return capital * risk_per_trade

def stop_loss(entry, atr):
    return entry - 1.5 * atr