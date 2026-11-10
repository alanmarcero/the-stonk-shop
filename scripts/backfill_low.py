#!/usr/bin/env python3
"""Backfill the 52-week daily closing low ('low') for all symbols in results files.

The calculation for low is the 52-week daily closing low price (or the lowest
recorded price if the symbol does not have 52 weeks of price data).
"""

import json
from pathlib import Path


def backfill_dict(stat: dict) -> bool:
    """Ensure 'low' is present on a stat dict, derived from 'low52wk' or closes."""
    changed = False
    if "low52wk" in stat:
        val = round(float(stat["low52wk"]), 2)
        if stat.get("low") != val:
            stat["low"] = val
            changed = True
    elif "low" in stat and "low52wk" not in stat:
        val = round(float(stat["low"]), 2)
        stat["low52wk"] = val
        changed = True
    return changed


def backfill_file(path: Path) -> int:
    """Backfill low in a JSON file. Returns count of modified records."""
    if not path.exists():
        print(f"Skipping {path} (not found)")
        return 0

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    modified = 0
    if isinstance(data, dict):
        if "stats" in data and isinstance(data["stats"], list):
            for item in data["stats"]:
                if isinstance(item, dict) and backfill_dict(item):
                    modified += 1
        elif "low52wk" in data:
            if backfill_dict(data):
                modified += 1

    if modified > 0:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
        print(f"Updated {path.name}: backfilled {modified} symbols")
    else:
        print(f"{path.name} already up to date")

    return modified


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    results_dir = repo_root / "results"

    total = 0
    for path in sorted(results_dir.glob("*.json")):
        total += backfill_file(path)

    print(f"\nBackfill complete. Total symbol records updated: {total}")


if __name__ == "__main__":
    main()
