from datetime import datetime, timezone
from typing import Optional

QUARTER_END_MONTHS = {3, 6, 9, 12}


def compute_quarterly_changes(
    closes: list[float], timestamps: list[int]
) -> Optional[dict]:
    """Compute since-quarter and during-quarter % changes.

    Returns {sinceQuarter: {"Q4'25": pct, ...}, duringQuarter: {"Q4'25": pct, ...}}
    or None if insufficient data.
    """
    if len(closes) < 2 or len(closes) != len(timestamps):
        return None

    quarter_ends = _extract_quarter_ends(closes, timestamps)
    if not quarter_ends:
        return None

    current_close = closes[-1]
    since_quarter = {}
    during_quarter = {}

    for i, qe in enumerate(quarter_ends):
        label = qe["label"]
        since_quarter[label] = round((current_close - qe["close"]) / qe["close"] * 100, 2) if qe["close"] > 0 else 0.0

        if i > 0:
            prev_close = quarter_ends[i - 1]["close"]
            during_quarter[label] = round((qe["close"] - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0

    return {"sinceQuarter": since_quarter, "duringQuarter": during_quarter}


def _extract_quarter_ends(
    closes: list[float], timestamps: list[int]
) -> list[dict]:
    """Extract last trading day close per quarter-end month."""
    monthly_last: dict[tuple[int, int], tuple[float, int]] = {}

    for close, ts in zip(closes, timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        if dt.month in QUARTER_END_MONTHS:
            monthly_last[(dt.year, dt.month)] = (close, ts)

    # Don't include current quarter-end if it's still in progress
    now_dt = datetime.fromtimestamp(timestamps[-1], tz=timezone.utc)
    if now_dt.month in QUARTER_END_MONTHS:
        monthly_last.pop((now_dt.year, now_dt.month), None)

    return [
        {"label": _quarter_label(month, year), "close": close, "ts": ts}
        for (year, month), (close, ts) in sorted(monthly_last.items())
    ]


def _quarter_label(month: int, year: int) -> str:
    """Format quarter label like Q4'25."""
    quarter_num = {3: 1, 6: 2, 9: 3, 12: 4}[month]
    return f"Q{quarter_num}'{str(year)[-2:]}"
