import json
from src.worker import yahoo, ema, app

def verify_symbol(symbol, expected_weeks_below):
    print(f"Verifying {symbol}...")
    weekly_result = yahoo.fetch_quarterly_candles(symbol)
    if not weekly_result:
        print(f"Failed to fetch data for {symbol}")
        return

    closes, timestamps, name = weekly_result
    
    # Strip incomplete week
    closes_stripped, timestamps_stripped = app._strip_incomplete_week(closes, timestamps)
    
    # Calculate EMA
    ema_value = ema.calculate(closes_stripped)
    
    # Detect crossover
    weeks_below = ema.detect_weekly_crossover(closes_stripped)
    
    last_close = closes_stripped[-1]
    
    print(f"Name: {name}")
    print(f"Last Close: {last_close}")
    print(f"EMA: {ema_value}")
    print(f"Weeks Below (detected): {weeks_below}")
    print(f"Expected Weeks Below: {expected_weeks_below}")
    
    if weeks_below == expected_weeks_below:
        print("MATCH!")
    else:
        print("MISMATCH!")
    
    # Print the last few closes vs EMA
    ema_series = ema._build_ema_series(closes_stripped, 5)
    ema_offset = 4
    from datetime import datetime
    for i in range(-5, 0):
        c = closes_stripped[i]
        e = ema_series[len(ema_series) + i]
        ts = timestamps_stripped[i]
        dt = datetime.fromtimestamp(ts)
        status = "ABOVE" if c > e * (1 + ema.BUFFER) else "BELOW"
        print(f"  Week {i}: Date={dt.strftime('%Y-%m-%d')}, Close={c:.2f}, EMA={e:.2f}, Status={status}")

if __name__ == "__main__":
    verify_symbol("CAR", 21)
    print("-" * 20)
    verify_symbol("LOGI", 15)
    print("-" * 20)
    verify_symbol("CHGG", 12)
