from typing import Optional

DEFAULT_PERIOD = 5
BUFFER = 0.01  # 1% threshold to avoid false signals from noise near the EMA line


def _build_ema_series(closes: list[float], period: int) -> list[float]:
    sma = sum(closes[:period]) / period
    multiplier = 2.0 / (period + 1)
    ema_series = [sma]
    for i in range(period, len(closes)):
        ema_series.append((closes[i] - ema_series[-1]) * multiplier + ema_series[-1])
    return ema_series


def calculate(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[float]:
    """Calculate the current EMA value. Returns None if insufficient data."""
    if len(closes) < period:
        return None
    return _build_ema_series(closes, period)[-1]


def detect_weekly_crossover(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[int]:
    """Detect if price just crossed above EMA. Returns weeks below before crossover, or None."""
    if (len(closes) < period + 1 or 
        (ema_series := _build_ema_series(closes, period)) and
        closes[period - 1 + (last_idx := len(ema_series) - 1)] <= ema_series[last_idx] * (1 + BUFFER) or
        closes[period - 1 + last_idx - 1] > ema_series[last_idx - 1]):
        return None

    return _count_consecutive(closes, ema_series, period - 1, last_idx - 1, lambda c, e: c <= e * (1 + BUFFER))


def detect_weekly_crossdown(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[int]:
    """Detect if price just crossed below EMA. Returns weeks above before crossdown, or None."""
    if (len(closes) < period + 1 or
        (ema_series := _build_ema_series(closes, period)) and
        closes[period - 1 + (last_idx := len(ema_series) - 1)] >= ema_series[last_idx] * (1 - BUFFER) or
        closes[period - 1 + last_idx - 1] < ema_series[last_idx - 1]):
        return None

    return _count_consecutive(closes, ema_series, period - 1, last_idx - 1, lambda c, e: c >= e * (1 - BUFFER))


def count_periods_below(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[int]:
    """Count consecutive periods the price has been at or below EMA. Returns None if above."""
    if (len(closes) < period + 1 or
        (ema_series := _build_ema_series(closes, period)) and
        closes[period - 1 + (last_idx := len(ema_series) - 1)] > ema_series[last_idx] * (1 + BUFFER)):
        return None

    return _count_consecutive(closes, ema_series, period - 1, last_idx, lambda c, e: c <= e * (1 + BUFFER))


def count_periods_above(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[int]:
    """Count consecutive periods the price has been above EMA. Returns None if at or below."""
    if (len(closes) < period + 1 or
        (ema_series := _build_ema_series(closes, period)) and
        closes[period - 1 + (last_idx := len(ema_series) - 1)] <= ema_series[last_idx] * (1 + BUFFER)):
        return None

    return _count_consecutive(closes, ema_series, period - 1, last_idx, lambda c, e: c >= e * (1 - BUFFER))


def _count_consecutive(closes: list[float], ema_series: list[float], offset: int, start_idx: int, condition) -> int:
    count = 0
    for i in range(start_idx, -1, -1):
        if not condition(closes[offset + i], ema_series[i]):
            break
        count += 1
    return count if count > 0 else 1 # Ensure at least 1 if we're calling this after initial checks

