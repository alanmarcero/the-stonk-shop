from datetime import datetime, timezone
from typing import Optional


THRESHOLD = 0.10  # 10% reversal required to register a significant swing level


def analyze(
    closes: list[float], timestamps: list[int]
) -> Optional[dict]:
    """Detect significant swing breakout/breakdown levels.

    Returns dict with breakout/breakdown price, date, and pct from current
    close, or None if no significant swings found.
    """
    if not closes or len(closes) != len(timestamps):
        return None

    breakout = _find_swing(closes, timestamps, "high")
    breakdown = _find_swing(closes, timestamps, "low")

    if breakout is None and breakdown is None:
        return None

    result = {}
    if breakout is not None:
        result["breakoutPrice"] = round(breakout["price"], 2)
        result["breakoutDate"] = breakout["date"]
        result["breakoutPct"] = round(
            (closes[-1] - breakout["price"]) / breakout["price"] * 100, 2
        )

    if breakdown is not None:
        result["breakdownPrice"] = round(breakdown["price"], 2)
        result["breakdownDate"] = breakdown["date"]
        result["breakdownPct"] = round(
            (closes[-1] - breakdown["price"]) / breakdown["price"] * 100, 2
        )

    return result


def _find_swing(
    closes: list[float], timestamps: list[int], direction: str,
) -> Optional[dict]:
    """Find last significant swing level in the given direction.

    direction='high' finds breakout levels; direction='low' finds breakdown levels.
    """
    find_high = direction == "high"
    significant = []
    running = closes[0]
    running_index = 0

    for i, close in enumerate(closes):
        if (find_high and close > running) or (not find_high and close < running):
            running = close
            running_index = i

        if running <= 0:
            continue

        reversal = abs(running - close) / running
        if reversal >= THRESHOLD:
            significant.append({"price": running, "index": running_index})
            running = close
            running_index = i

    if not significant:
        return None

    last = significant[-1]
    dt = datetime.fromtimestamp(timestamps[last["index"]], tz=timezone.utc)
    return {"price": last["price"], "date": _format_date(dt)}


def _format_date(dt: datetime) -> str:
    """Format date as M/D/YY without zero-padding (cross-platform)."""
    return f"{dt.month}/{dt.day}/{dt.strftime('%y')}"
