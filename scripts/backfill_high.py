#!/usr/bin/env python3
"""Backfill the 52-week daily closing high ('high' and 'high52wk') for all symbols in results files.

The calculation for high is the 52-week daily closing high price (or the highest
recorded price if the symbol does not have 52 weeks of price data).
"""

from __future__ import annotations

import concurrent.futures
import json
from pathlib import Path
import sys
import time
from typing import Optional

# Ensure repo root is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.worker.yahoo import _fetch_candles
from src.worker.stats import compute_high


def fetch_symbol_high(symbol: str, retries: int = 2) -> tuple[str, float | None, list[float] | None]:
    """Fetch 1y daily candles and compute 52-week high."""
    for attempt in range(retries + 1):
        candles = _fetch_candles(symbol, range_param="1y", interval="1d")
        if candles and candles[0]:
            closes = candles[0]
            val = compute_high(closes)
            if val is not None:
                return symbol, val, closes
        if attempt < retries:
            time.sleep(0.2)
    return symbol, None, None


def main() -> None:
    results_dir = REPO_ROOT / "results"
    latest_stats_path = results_dir / "latest-stats.json"

    if not latest_stats_path.exists():
        print(f"Error: {latest_stats_path} not found")
        sys.exit(1)

    with open(latest_stats_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    stats_list = data.get("stats", [])
    print(f"Loaded {len(stats_list)} symbols from latest-stats.json")

    # Gather symbols
    symbols = [s["symbol"] for s in stats_list if "symbol" in s]

    # Fetch 52w high in parallel
    print(f"Fetching 1y daily candles for {len(symbols)} symbols...")
    symbol_high_map: dict[str, float] = {}
    symbol_closes_map: dict[str, list[float]] = {}

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(fetch_symbol_high, sym): sym for sym in symbols}
        done_count = 0
        for future in concurrent.futures.as_completed(futures):
            sym, high_val, closes = future.result()
            if high_val is not None:
                symbol_high_map[sym] = high_val
                if closes:
                    symbol_closes_map[sym] = closes
            done_count += 1
            if done_count % 500 == 0 or done_count == len(symbols):
                elapsed = time.time() - start_time
                print(f"  Processed {done_count}/{len(symbols)} symbols in {elapsed:.1f}s (found high for {len(symbol_high_map)})")

    print(f"Fetched 52w high for {len(symbol_high_map)}/{len(symbols)} symbols.")

    # Apply backfill to latest-stats.json
    modified_count = 0
    fallback_count = 0
    for stat in stats_list:
        sym = stat.get("symbol")
        high_val = symbol_high_map.get(sym)

        if high_val is None:
            # Fallback to high3yr or close if candle fetch failed (e.g. delisted/OTC)
            high_val = stat.get("high3yr") or stat.get("close")
            if high_val is not None:
                high_val = round(float(high_val), 2)
                fallback_count += 1

        if high_val is not None:
            close = stat.get("close", high_val)
            if close > high_val:
                high_val = round(close, 2)

            pct = round((close - high_val) / high_val * 100, 2) if high_val > 0 else 0.0

            stat["high"] = round(high_val, 2)
            stat["high52wk"] = round(high_val, 2)
            stat["highPct"] = pct
            modified_count += 1

    # Recalculate breadth stat pctWithin5OfHigh
    h_pcts = [s["highPct"] for s in stats_list if "highPct" in s]
    if "misc" in data and h_pcts:
        data["misc"]["pctWithin5OfHigh"] = round(len([p for p in h_pcts if p >= -5.0]) / len(h_pcts) * 100, 2)
        print(f"Updated misc.pctWithin5OfHigh: {data['misc']['pctWithin5OfHigh']}%")

    with open(latest_stats_path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"Saved {latest_stats_path.name}: {modified_count} symbols updated ({fallback_count} fallbacks)")

    # Backfill batch files
    for batch_path in sorted(results_dir.glob("batch-*.json")):
        with open(batch_path, "r", encoding="utf-8") as f:
            batch_data = json.load(f)

        batch_modified = 0
        for item in batch_data.get("stats", []):
            sym = item.get("symbol")
            high_val = symbol_high_map.get(sym) or item.get("high3yr") or item.get("close")
            if high_val is not None:
                high_val = round(float(high_val), 2)
                close = item.get("close", high_val)
                if close > high_val:
                    high_val = round(close, 2)
                pct = round((close - high_val) / high_val * 100, 2) if high_val > 0 else 0.0
                item["high"] = round(high_val, 2)
                item["high52wk"] = round(high_val, 2)
                item["highPct"] = pct
                batch_modified += 1

        if batch_modified > 0:
            with open(batch_path, "w", encoding="utf-8") as f:
                json.dump(batch_data, f, separators=(",", ":"))
            print(f"Updated {batch_path.name}: {batch_modified} symbols backfilled")

    print("\nBackfill complete!")


if __name__ == "__main__":
    main()
