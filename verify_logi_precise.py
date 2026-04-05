from src.worker import ema, yahoo, app
from datetime import datetime

def verify_logi():
    # Fetch weekly data
    res = yahoo.fetch_quarterly_candles("LOGI")
    closes, timestamps, name = res
    
    # We want to simulate the run on Friday March 27 at noon.
    # At that time, Yahoo returned 92.58 as the close for the week of March 23.
    # So we'll find the week of March 16 and add a candle for March 23 with 92.58.
    
    idx = -1
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts)
        if dt.strftime('%Y-%m-%d') == '2026-03-16':
            idx = i
            break
    
    if idx == -1:
        print("Could not find March 16 candle")
        return

    # Data up to March 16 (close of March 20 was 87.92)
    base_closes = closes[:idx+1]
    
    # Add the "current" week's price (Thursday's close)
    test_closes = base_closes + [92.58]
    
    ema_val = ema.calculate(test_closes)
    weeks_below = ema.detect_weekly_crossover(test_closes)
    
    print(f"Symbol: LOGI")
    print(f"Simulated Close (Thursday): 92.58")
    print(f"Calculated 5W EMA: {ema_val}")
    print(f"Weeks Below: {weeks_below}")
    
    # Let's see the EMA series for the last few weeks
    ema_series = ema._build_ema_series(test_closes, 5)
    for i in range(-5, 0):
        c = test_closes[i]
        e = ema_series[len(ema_series) + i]
        print(f"  Week {i}: Close={c:.2f}, EMA={e:.2f}")

if __name__ == "__main__":
    verify_logi()
