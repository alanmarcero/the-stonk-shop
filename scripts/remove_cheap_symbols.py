import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SYMBOLS_PATH = os.path.join(SCRIPT_DIR, "..", "symbols", "us-equities.txt")
CHEAP_SYMBOLS_LOG = os.path.join(SCRIPT_DIR, "cheap_symbols.json")

YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
TIMEOUT_SECONDS = 10
MAX_WORKERS = 15
PRICE_THRESHOLD = 3.0


def fetch_current_price(symbol: str) -> Tuple[str, Optional[float]]:
    """Fetch current price for a symbol from Yahoo Finance."""
    url = f"{YAHOO_BASE_URL}/{symbol}?range=1d&interval=1m"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            data = json.loads(response.read())
    except Exception as err:
        # print(f"[error] {symbol}: {err}")
        return symbol, None

    try:
        chart = data["chart"]["result"][0]
        price = chart["meta"].get("regularMarketPrice")
        if price is None:
            # Try to get last close from indicators if meta is missing it
            closes = chart["indicators"]["quote"][0].get("close", [])
            valid_closes = [c for c in closes if c is not None]
            if valid_closes:
                price = valid_closes[-1]
        
        return symbol, price
    except (KeyError, IndexError, TypeError) as err:
        # print(f"[error] {symbol} parse: {err}")
        return symbol, None


def main():
    if not os.path.exists(SYMBOLS_PATH):
        print(f"Error: {SYMBOLS_PATH} not found.")
        return

    symbol_data = {}
    with open(SYMBOLS_PATH, "r") as f:
        for line in f:
            if line.strip():
                parts = line.strip().split(",")
                s = parts[0]
                cap = parts[1] if len(parts) > 1 else "0"
                symbol_data[s] = cap

    all_symbols = list(symbol_data.keys())
    print(f"Loaded {len(all_symbols)} symbols.")
    print(f"Fetching current prices using {MAX_WORKERS} workers...")

    failed = []
    cheap = []
    valid = []

    done = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_current_price, s): s for s in all_symbols}
        for future in as_completed(futures):
            symbol, price = future.result()
            done += 1
            
            if price is None:
                failed.append(symbol)
            elif price < PRICE_THRESHOLD:
                cheap.append(symbol)
            else:
                valid.append(symbol)

            if done % 100 == 0:
                elapsed = time.time() - start_time
                rate = done / elapsed
                remaining = (len(all_symbols) - done) / rate
                print(f"  Progress: {done}/{len(all_symbols)} | Cheap: {len(cheap)} | Failed: {len(failed)} | ETA: {remaining/60:.1f}m")

    print(f"\n--- Pruning Summary ---")
    print(f"Total symbols:   {len(all_symbols)}")
    print(f"Kept (>= $3):    {len(valid)}")
    print(f"Removed (< $3):  {len(cheap)}")
    print(f"Failed to fetch: {len(failed)}")

    # We keep the failed ones for now to avoid accidental deletion of good symbols due to transient errors
    # unless they are known to be problematic. For this task, we focus on removing those definitely < $3.
    new_symbols = sorted(valid + failed)

    with open(SYMBOLS_PATH, "w", newline="\n") as f:
        for s in new_symbols:
            f.write(f"{s},{symbol_data[s]}\n")

    with open(CHEAP_SYMBOLS_LOG, "w") as f:
        json.dump({"cheap": cheap, "failed": failed}, f, indent=2)

    print(f"\nUpdated {SYMBOLS_PATH}")
    print(f"Saved removed symbols to {CHEAP_SYMBOLS_LOG}")


if __name__ == "__main__":
    main()
