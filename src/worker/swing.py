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

    breakout = _find_breakout(closes, timestamps)
    breakdown = _find_breakdown(closes, timestamps)

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


def _find_breakout(
    closes: list[float], timestamps: list[int]
) -> Optional[dict]:
    """Find last significant high (breakout level)."""
    significant_highs = []
    running_max = closes[0]
    running_max_index = 0

    for i, close in enumerate(closes):
        if close > running_max:
            running_max = close
            running_max_index = i

        decline = (running_max - close) / running_max if running_max > 0 else 0
        if decline >= THRESHOLD:
            significant_highs.append(
                {"price": running_max, "index": running_max_index}
            )
            running_max = close
            running_max_index = i

    if not significant_highs:
        return None

    last = significant_highs[-1]
    dt = datetime.fromtimestamp(timestamps[last["index"]], tz=timezone.utc)
    return {"price": last["price"], "date": _format_date(dt)}


def _format_date(dt: datetime) -> str:
    """Format date as M/D/YY without zero-padding (cross-platform)."""
    return f"{dt.month}/{dt.day}/{dt.strftime('%y')}"


def _find_breakdown(
    closes: list[float], timestamps: list[int]
) -> Optional[dict]:
    """Find last significant low (breakdown level)."""
    significant_lows = []
    running_min = closes[0]
    running_min_index = 0

    for i, close in enumerate(closes):
        if close < running_min:
            running_min = close
            running_min_index = i

        if running_min <= 0:
            continue

        rise = (close - running_min) / running_min
        if rise >= THRESHOLD:
            significant_lows.append(
                {"price": running_min, "index": running_min_index}
            )
            running_min = close
            running_min_index = i

    if not significant_lows:
        return None

    last = significant_lows[-1]
    dt = datetime.fromtimestamp(timestamps[last["index"]], tz=timezone.utc)
    return {"price": last["price"], "date": _format_date(dt)}
