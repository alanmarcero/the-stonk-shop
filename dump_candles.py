import json
import urllib.request
from datetime import datetime

BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
USER_AGENT = "Mozilla/5.0"

def dump_candles(symbol):
    url = f"{BASE_URL}/{symbol}?range=1y&interval=1wk"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request) as response:
        data = json.loads(response.read())
    
    result = data["chart"]["result"][0]
    timestamps = result["timestamp"]
    closes = result["indicators"]["quote"][0]["close"]
    
    print(f"Candles for {symbol}:")
    for ts, close in zip(timestamps[-10:], closes[-10:]):
        dt = datetime.fromtimestamp(ts)
        print(f"  {dt.strftime('%Y-%m-%d')}: {close}")

if __name__ == "__main__":
    dump_candles("AAP")
