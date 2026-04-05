from src.worker import ema, yahoo, app
from datetime import datetime

def check_ema(symbol):
    weekly_result = yahoo.fetch_quarterly_candles(symbol)
    closes, timestamps, name = weekly_result
    
    # Strip current week
    # If we want to simulate the scan on March 27 (Friday).
    # We should have candles up to March 23 (Monday).
    # _strip_incomplete_week on March 27 would strip March 23.
    # Leaving March 16 as the last candle.
    
    # Let's find the candle for Feb 2.
    idx = -1
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts)
        if dt.strftime('%Y-%m-%d') == '2026-02-02':
            idx = i
            break
    
    if idx == -1:
        print("Could not find Feb 2 candle")
        return

    test_closes = closes[:idx+1]
    last_close = test_closes[-1]
    
    ema_series = ema._build_ema_series(test_closes, 5)
    current_ema = ema_series[-1]
    
    print(f"Symbol: {symbol}")
    print(f"Simulated Last Date: 2026-02-02 (Week of)")
    print(f"Close: {last_close}")
    print(f"Calculated 5W EMA: {current_ema}")
    
    weeks_below = ema.detect_weekly_crossover(test_closes)
    print(f"Detected Weeks Below: {weeks_below}")

if __name__ == "__main__":
    check_ema("ALNY")
