from src.worker import ema, yahoo, app
from datetime import datetime

def verify_car():
    # Fetch weekly data
    res = yahoo.fetch_quarterly_candles("CAR")
    closes, timestamps, name = res
    
    idx = -1
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts)
        if dt.strftime('%Y-%m-%d') == '2026-03-16':
            idx = i
            break
    
    if idx == -1:
        print("Could not find March 16 candle")
        return

    # Data up to March 16 (close of March 20 was 99.90)
    base_closes = closes[:idx+1]
    
    # Add the "current" week's price (Thursday's close for CAR was 139.58)
    test_closes = base_closes + [139.58]
    
    ema_val = ema.calculate(test_closes)
    weeks_below = ema.detect_weekly_crossover(test_closes)
    
    print(f"Symbol: CAR")
    print(f"Simulated Close (Thursday): 139.58")
    print(f"Calculated 5W EMA: {ema_val}")
    print(f"Weeks Below: {weeks_below}")

if __name__ == "__main__":
    verify_car()
