"""Prune dead and low-volume symbols from the scanner symbol list.

Downloads error logs from S3 to find failed symbols, then checks Yahoo
Finance volume for the rest.  Symbols that failed the last scan or average
< 250k daily volume over the past month are removed.

Usage:
    python prune_symbols.py [--dry-run]
"""

import argparse
import json
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SYMBOLS_PATH = os.path.join(SCRIPT_DIR, "..", "symbols", "us-equities.txt")

S3_BUCKET = "ema-scanner-ced843a6"
S3_LOG_PREFIX = "logs/2026-02-28/"
S3_SYMBOLS_KEY = "symbols/us-equities.txt"

YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
USER_AGENT = "Mozilla/5.0"
TIMEOUT_SECONDS = 10

MIN_AVG_VOLUME = 250_000
MAX_WORKERS = 10


def load_symbols() -> list[str]:
    with open(SYMBOLS_PATH) as f:
        return [line.strip() for line in f if line.strip()]


def fetch_failed_symbols(s3) -> set[str]:
    """Download all error logs from S3 and collect failed symbols."""
    paginator = s3.get_paginator("list_objects_v2")
    failed = set()

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_LOG_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
            errors = json.loads(resp["Body"].read())
            for entry in errors:
                failed.add(entry["symbol"])

    return failed


def fetch_avg_volume(symbol: str) -> tuple[str, float | None]:
    """Fetch average daily volume for a symbol over the past month."""
    url = f"{YAHOO_BASE_URL}/{symbol}?range=1mo&interval=1d"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            data = json.loads(response.read())
    except (OSError, ValueError):
        return symbol, None

    try:
        volumes = data["chart"]["result"][0]["indicators"]["quote"][0]["volume"]
        valid = [v for v in volumes if v is not None]
    except (KeyError, IndexError, TypeError):
        return symbol, None

    if not valid:
        return symbol, None

    return symbol, sum(valid) / len(valid)


def main():
    parser = argparse.ArgumentParser(description="Prune dead/low-volume symbols")
    parser.add_argument("--dry-run", action="store_true", help="Report only, don't write")
    args = parser.parse_args()

    # Load current symbols
    all_symbols = load_symbols()
    print(f"Loaded {len(all_symbols)} symbols from {SYMBOLS_PATH}")

    # Fetch failed symbols from S3
    session = boto3.Session(profile_name="scanner", region_name="us-east-1")
    s3 = session.client("s3")

    print("Fetching error logs from S3...")
    failed = fetch_failed_symbols(s3)
    print(f"Found {len(failed)} failed symbols")

    # Remove failed symbols
    remaining = [s for s in all_symbols if s not in failed]
    print(f"After removing failed: {len(remaining)} symbols")

    # Fetch volume for remaining symbols
    print(f"Fetching volume for {len(remaining)} symbols ({MAX_WORKERS} workers)...")
    low_volume = set()
    fetch_failed = set()
    done = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_avg_volume, s): s for s in remaining}
        for future in as_completed(futures):
            symbol, avg_vol = future.result()
            done += 1
            if done % 500 == 0:
                print(f"  Progress: {done}/{len(remaining)}")

            if avg_vol is None:
                fetch_failed.add(symbol)
            elif avg_vol < MIN_AVG_VOLUME:
                low_volume.add(symbol)

    # Build pruned list (preserve original order)
    removed = failed | low_volume | fetch_failed
    pruned = [s for s in all_symbols if s not in removed]

    # Summary
    print(f"\n--- Summary ---")
    print(f"Original symbols:        {len(all_symbols)}")
    print(f"Removed (scan failed):   {len(failed)}")
    print(f"Removed (volume failed): {len(fetch_failed)}")
    print(f"Removed (low volume):    {len(low_volume)}")
    print(f"Total removed:           {len(removed)}")
    print(f"Remaining symbols:       {len(pruned)}")

    if args.dry_run:
        print("\n[DRY RUN] No files written.")
        return

    # Write pruned list
    with open(SYMBOLS_PATH, "w", newline="\n") as f:
        for symbol in pruned:
            f.write(symbol + "\n")
    print(f"\nWrote {len(pruned)} symbols to {SYMBOLS_PATH}")

    # Upload to S3
    s3.upload_file(SYMBOLS_PATH, S3_BUCKET, S3_SYMBOLS_KEY)
    print(f"Uploaded to s3://{S3_BUCKET}/{S3_SYMBOLS_KEY}")


if __name__ == "__main__":
    main()
