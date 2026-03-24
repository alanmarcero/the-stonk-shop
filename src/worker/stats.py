from datetime import datetime, timezone
from typing import Optional

# Dual import supports both Lambda (package) and direct test execution contexts.
try:
    from . import swing, rsi, quarterly, vix
except ImportError:
    import swing, rsi, quarterly, vix


def compute_ytd_pct(closes: list[float], timestamps: list[int]) -> Optional[float]:
    """Return YTD % change from last close of previous year to current close."""
    if len(closes) < 2:
        return None

    current_year = datetime.fromtimestamp(timestamps[-1], tz=timezone.utc).year
    prev_year = current_year - 1

    dec31_close = None
    for close, ts in zip(closes, timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        if dt.year == prev_year:
            dec31_close = close

    if dec31_close is None:
        return None

    return round((closes[-1] - dec31_close) / dec31_close * 100, 2)


def compute_highest_close_pct(closes: list[float]) -> Optional[tuple[float, float]]:
    """Return (pct from 3yr high, the high value). Pct is negative or zero."""
    if not closes:
        return None
    high = max(closes)
    if high <= 0:
        return 0.0, high
    pct = round((closes[-1] - high) / high * 100, 2)
    return pct, high


def compute_lowest_close_pct(closes: list[float]) -> Optional[tuple[float, float]]:
    """Return (pct above 52wk low, the low value) using last 252 closes."""
    if not closes:
        return None
    window = closes[-252:] if len(closes) >= 252 else closes
    low = min(window)
    if low <= 0:
        return 0.0, low
    pct = round((closes[-1] - low) / low * 100, 2)
    return pct, low


def compute_return_since(
    closes: list[float],
    timestamps: list[int],
    year: int,
    month: int,
    day: int,
) -> Optional[float]:
    """Return % change from close on/near target date to current close."""
    if len(closes) < 2:
        return None
    target = datetime(year, month, day, tzinfo=timezone.utc).timestamp()
    best_idx = None
    best_diff = float("inf")
    three_days = 3 * 86400
    for i, ts in enumerate(timestamps):
        diff = abs(ts - target)
        if diff < best_diff and diff <= three_days:
            best_diff = diff
            best_idx = i
    if best_idx is None or closes[best_idx] == 0:
        return None
    return round((closes[-1] - closes[best_idx]) / closes[best_idx] * 100, 2)


def compute_stats(
    closes: list[float],
    timestamps: list[int],
    vix_spikes: Optional[list[dict]] = None,
    forward_pe: Optional[float] = None,
    forward_pe_history: Optional[dict] = None,
    weekly_closes: Optional[list[float]] = None,
) -> Optional[dict]:
    """Compute all stats for a symbol. Returns None if insufficient data."""
    if len(closes) < 2:
        return None

    stats: dict = {"close": round(closes[-1], 2)}

    ytd = compute_ytd_pct(closes, timestamps)
    if ytd is not None:
        stats["ytdPct"] = ytd

    high_result = compute_highest_close_pct(closes)
    if high_result is not None:
        stats["highPct"] = high_result[0]
        stats["high3yr"] = round(high_result[1], 2)

    low_result = compute_lowest_close_pct(closes)
    if low_result is not None:
        stats["lowPct"] = low_result[0]
        stats["low52wk"] = round(low_result[1], 2)

    # Swing levels
    swing_result = swing.analyze(closes, timestamps)
    if swing_result is not None:
        stats.update(swing_result)

    # RSI
    rsi_value = rsi.calculate(closes)
    if rsi_value is not None:
        stats["rsi"] = rsi_value

    # Quarterly changes
    q_result = quarterly.compute_quarterly_changes(closes, timestamps)
    if q_result is not None:
        stats["sinceQuarter"] = q_result["sinceQuarter"]
        stats["duringQuarter"] = q_result["duringQuarter"]

    # VIX spike returns
    if vix_spikes:
        vix_returns = vix.compute_spike_returns(
            vix_spikes, closes, timestamps, closes[-1]
        )
        if vix_returns:
            stats["vixReturns"] = vix_returns

    # 200-day SMA
    if len(closes) >= 200:
        sma200d = round(sum(closes[-200:]) / 200, 2)
        stats["sma200d"] = sma200d
        if sma200d > 0:
            stats["pctSma200d"] = round((closes[-1] - sma200d) / sma200d * 100, 2)
        else:
            stats["pctSma200d"] = 0.0

    # 200-week SMA
    if weekly_closes is not None and len(weekly_closes) >= 200:
        sma200w = round(sum(weekly_closes[-200:]) / 200, 2)
        stats["sma200w"] = sma200w
        if sma200w > 0:
            stats["pctSma200w"] = round((closes[-1] - sma200w) / sma200w * 100, 2)
        else:
            stats["pctSma200w"] = 0.0

    # Forward P/E
    if forward_pe is not None:
        stats["forwardPE"] = forward_pe
    if forward_pe_history is not None:
        stats["forwardPEHistory"] = forward_pe_history

    return stats
