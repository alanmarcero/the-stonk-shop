import json
import os
import urllib.request
from typing import List, Dict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SYMBOLS_PATH = os.path.join(SCRIPT_DIR, "..", "symbols", "us-equities.txt")

# Nasdaq API settings
NASDAQ_URL = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
CAP_THRESHOLD = 5_000_000_000  # $5 Billion


def fetch_nasdaq_data() -> Dict[str, int]:
    print(f"Fetching Nasdaq data from {NASDAQ_URL}...")
    request = urllib.request.Request(NASDAQ_URL, headers={"User-Agent": USER_AGENT})
    
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            data = json.loads(response.read())
            rows = data.get("data", {}).get("table", {}).get("rows", [])
            
            market_caps = {}
            for row in rows:
                symbol = row.get("symbol")
                cap_str = row.get("marketCap", "").replace(",", "")
                if symbol and cap_str:
                    try:
                        market_caps[symbol] = int(float(cap_str))
                    except ValueError:
                        pass
            return market_caps
    except Exception as e:
        print(f"Error fetching Nasdaq data: {e}")
        return {}


def main():
    if not os.path.exists(SYMBOLS_PATH):
        print(f"Error: {SYMBOLS_PATH} not found.")
        return

    # Load existing symbols (currently pruned to > $5B or not found)
    with open(SYMBOLS_PATH, "r") as f:
        existing_symbols = [line.strip().split(',')[0] for line in f if line.strip()]

    nasdaq_caps = fetch_nasdaq_data()
    if not nasdaq_caps:
        print("Failed to fetch Nasdaq data.")
        return

    new_lines = []
    removed_count = 0
    kept_count = 0

    for symbol in existing_symbols:
        cap = nasdaq_caps.get(symbol)
        if cap is not None:
            if cap < CAP_THRESHOLD:
                # Should have been pruned already, but double check
                removed_count += 1
                continue
            new_lines.append(f"{symbol},{cap}")
            kept_count += 1
        else:
            # Not in Nasdaq (likely an ETF or foreign), keep with 0 or null
            # Let's use 0 to indicate unknown/ETF
            new_lines.append(f"{symbol},0")
            kept_count += 1

    print(f"Kept: {kept_count}, Removed (below threshold): {removed_count}")
    
    with open(SYMBOLS_PATH, "w", newline="\n") as f:
        for line in new_lines:
            f.write(line + "\n")
    
    print(f"Updated {SYMBOLS_PATH}")


if __name__ == "__main__":
    main()
