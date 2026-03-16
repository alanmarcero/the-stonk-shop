from datetime import datetime, timezone
from typing import Optional


QUARTER_END_MONTHS = {3, 6, 9, 12}

QUARTER_LABELS = {
    (3,): "Q1", (6,): "Q2", (9,): "Q3", (12,): "Q4",
}


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
        since_quarter[label] = round(
            (current_close - qe["close"]) / qe["close"] * 100, 2
        )

        if i > 0:
            prev_close = quarter_ends[i - 1]["close"]
            during_quarter[label] = round(
                (qe["close"] - prev_close) / prev_close * 100, 2
            )

    return {"sinceQuarter": since_quarter, "duringQuarter": during_quarter}


def _extract_quarter_ends(
    closes: list[float], timestamps: list[int]
) -> list[dict]:
    """Extract last trading day close per quarter-end month."""
    monthly_last: dict[tuple[int, int], tuple[float, int]] = {}

    for close, ts in zip(closes, timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        if dt.month in QUARTER_END_MONTHS:
            key = (dt.year, dt.month)
            monthly_last[key] = (close, ts)

    # Don't include current quarter-end if it's still in progress
    now_ts = timestamps[-1]
    now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    current_key = (now_dt.year, now_dt.month)
    if now_dt.month in QUARTER_END_MONTHS:
        # Only exclude if we're still in the quarter-end month
        monthly_last.pop(current_key, None)

    result = []
    for (year, month), (close, ts) in sorted(monthly_last.items()):
        quarter = _quarter_label(month, year)
        result.append({"label": quarter, "close": close, "ts": ts})

    return result


def _quarter_label(month: int, year: int) -> str:
    """Format quarter label like Q4'25."""
    quarter_num = {3: 1, 6: 2, 9: 3, 12: 4}[month]
    short_year = str(year)[-2:]
    return f"Q{quarter_num}'{short_year}"
