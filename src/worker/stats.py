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

    curr_year = datetime.fromtimestamp(timestamps[-1], tz=timezone.utc).year
    # Find last close of previous year
    prev_closes = [c for c, ts in zip(closes, timestamps) if datetime.fromtimestamp(ts, tz=timezone.utc).year < curr_year]
    if not prev_closes:
        return None
    
    dec31_close = prev_closes[-1]
    return round((closes[-1] - dec31_close) / dec31_close * 100, 2)


def compute_highest_close_pct(closes: list[float]) -> Optional[tuple[float, float]]:
    """Return (pct from 3yr high, the high value). Pct is negative or zero."""
    if not closes:
        return None
    high = max(closes)
    return (round((closes[-1] - high) / high * 100, 2) if high > 0 else 0.0), high


def compute_lowest_close_pct(closes: list[float]) -> Optional[tuple[float, float]]:
    """Return (pct above 52wk low, the low value) using last 252 closes."""
    if not closes:
        return None
    window = closes[-252:]
    low = min(window)
    return (round((closes[-1] - low) / low * 100, 2) if low > 0 else 0.0), low


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
    three_days = 3 * 86400
    
    # Find best match within 3 days
    matches = [(abs(ts - target), i) for i, ts in enumerate(timestamps) if abs(ts - target) <= three_days]
    if not matches:
        return None
    
    _, best_idx = min(matches)
    if closes[best_idx] == 0:
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

    stats_res: dict = {"close": round(closes[-1], 2)}

    if (ytd := compute_ytd_pct(closes, timestamps)) is not None:
        stats_res["ytdPct"] = ytd

    if high_res := compute_highest_close_pct(closes):
        stats_res["highPct"], stats_res["high3yr"] = high_res[0], round(high_res[1], 2)

    if low_res := compute_lowest_close_pct(closes):
        stats_res["lowPct"], stats_res["low52wk"] = low_res[0], round(low_res[1], 2)

    if s_res := swing.analyze(closes, timestamps):
        stats_res.update(s_res)

    if (r_val := rsi.calculate(closes)) is not None:
        stats_res["rsi"] = r_val

    if q_res := quarterly.compute_quarterly_changes(closes, timestamps):
        stats_res.update({"sinceQuarter": q_res["sinceQuarter"], "duringQuarter": q_res["duringQuarter"]})

    if vix_spikes and (v_res := vix.compute_spike_returns(vix_spikes, closes, timestamps, closes[-1])):
        stats_res["vixReturns"] = v_res

    if len(closes) >= 200:
        sma = round(sum(closes[-200:]) / 200, 2)
        stats_res.update({"sma200d": sma, "pctSma200d": round((closes[-1] - sma) / sma * 100, 2) if sma > 0 else 0.0})

    if weekly_closes and len(weekly_closes) >= 200:
        smaw = round(sum(weekly_closes[-200:]) / 200, 2)
        stats_res.update({"sma200w": smaw, "pctSma200w": round((closes[-1] - smaw) / smaw * 100, 2) if smaw > 0 else 0.0})

    if forward_pe is not None: stats_res["forwardPE"] = forward_pe
    if forward_pe_history: stats_res["forwardPEHistory"] = forward_pe_history

    return stats_res
