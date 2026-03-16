from typing import Optional

DEFAULT_PERIOD = 5
BUFFER = 0.01  # 1% threshold to avoid false signals from noise near the EMA line


def _build_ema_series(closes: list[float], period: int) -> list[float]:
    sma = sum(closes[:period]) / period
    multiplier = 2.0 / (period + 1)
    ema = sma
    series = [ema]

    for i in range(period, len(closes)):
        ema = (closes[i] - ema) * multiplier + ema
        series.append(ema)

    return series


def calculate(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[float]:
    """Calculate the current EMA value. Returns None if insufficient data."""
    if len(closes) < period:
        return None

    return _build_ema_series(closes, period)[-1]


def detect_weekly_crossover(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[int]:
    """Detect if price just crossed above EMA. Returns weeks below before crossover, or None."""
    if len(closes) < period + 1:
        return None

    ema_series = _build_ema_series(closes, period)
    last_index = len(ema_series) - 1
    if last_index < 1:
        return None

    ema_offset = period - 1

    if closes[ema_offset + last_index] <= ema_series[last_index] * (1 + BUFFER):
        return None

    # Count consecutive weeks below EMA before the current candle
    weeks_below = 0
    for i in range(last_index - 1, -1, -1):
        if closes[ema_offset + i] > ema_series[i] * (1 + BUFFER):
            break
        weeks_below += 1

    return weeks_below if weeks_below > 0 else None


def detect_weekly_crossdown(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[int]:
    """Detect if price just crossed below EMA. Returns weeks above before crossdown, or None."""
    if len(closes) < period + 1:
        return None

    ema_series = _build_ema_series(closes, period)
    last_index = len(ema_series) - 1
    if last_index < 1:
        return None

    ema_offset = period - 1

    if closes[ema_offset + last_index] >= ema_series[last_index] * (1 - BUFFER):
        return None

    # Count consecutive weeks above EMA before the current candle
    weeks_above = 0
    for i in range(last_index - 1, -1, -1):
        if closes[ema_offset + i] < ema_series[i] * (1 - BUFFER):
            break
        weeks_above += 1

    return weeks_above if weeks_above > 0 else None


def count_periods_below(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[int]:
    """Count consecutive periods the price has been at or below EMA. Returns None if above."""
    if len(closes) < period + 1:
        return None

    ema_series = _build_ema_series(closes, period)
    last_index = len(ema_series) - 1
    if last_index < 0:
        return None

    ema_offset = period - 1

    # Stay "below" until price crosses the crossover threshold (> buffer above)
    if closes[ema_offset + last_index] > ema_series[last_index] * (1 + BUFFER):
        return None

    weeks_below = 1
    for i in range(last_index - 1, -1, -1):
        if closes[ema_offset + i] > ema_series[i] * (1 + BUFFER):
            break
        weeks_below += 1

    return weeks_below


def count_periods_above(closes: list[float], period: int = DEFAULT_PERIOD) -> Optional[int]:
    """Count consecutive periods the price has been above EMA. Returns None if at or below."""
    if len(closes) < period + 1:
        return None

    ema_series = _build_ema_series(closes, period)
    last_index = len(ema_series) - 1
    if last_index < 0:
        return None

    ema_offset = period - 1

    # Must be decisively above EMA (> buffer) to enter "above" list
    if closes[ema_offset + last_index] <= ema_series[last_index] * (1 + BUFFER):
        return None

    periods_above = 1
    for i in range(last_index - 1, -1, -1):
        if closes[ema_offset + i] < ema_series[i] * (1 - BUFFER):
            break
        periods_above += 1

    return periods_above
