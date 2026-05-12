from datetime import datetime, timezone
from typing import Optional


def detect_spikes(
    closes: list[float],
    timestamps: list[int],
    threshold: float = 20.0,
    gap_days: int = 5,
) -> list[dict]:
    """Detect VIX spike clusters. Returns list of {dateString, timestamp, vixClose}."""
    if len(closes) != len(timestamps) or not closes:
        return []

    spike_indices = [i for i, c in enumerate(closes) if c >= threshold]
    if not spike_indices:
        return []

    clusters: list[list[int]] = []
    current_cluster: list[int] = [spike_indices[0]]

    for i in range(1, len(spike_indices)):
        if spike_indices[i] - spike_indices[i - 1] <= gap_days + 1:
            current_cluster.append(spike_indices[i])
            continue
        
        clusters.append(current_cluster)
        current_cluster = [spike_indices[i]]
    
    clusters.append(current_cluster)

    return [
        _format_spike(cluster, closes, timestamps)
        for cluster in clusters
    ]


def _format_spike(cluster: list[int], closes: list[float], timestamps: list[int]) -> dict:
    peak_index = max(cluster, key=lambda idx: closes[idx])
    dt = datetime.fromtimestamp(timestamps[peak_index], tz=timezone.utc)
    return {
        "dateString": f"{dt.month}/{dt.day}/{dt.strftime('%y')}",
        "timestamp": timestamps[peak_index],
        "vixClose": round(closes[peak_index], 2),
    }


def compute_spike_returns(
    spikes: list[dict],
    closes: list[float],
    timestamps: list[int],
    current_close: float,
) -> list[dict]:
    """For each VIX spike, find symbol's close on that date and compute return.

    Returns list of {dateString, vixClose, spikeClose, pctGain}.
    """
    if not spikes or not closes or not timestamps:
        return []

    ts_to_close = dict(zip(timestamps, closes))

    results = []
    for spike in spikes:
        spike_close = _find_closest_close(spike["timestamp"], timestamps, ts_to_close)
        if spike_close is None or spike_close == 0:
            continue

        results.append({
            "dateString": spike["dateString"],
            "vixClose": spike["vixClose"],
            "spikeClose": round(spike_close, 2),
            "pctGain": round((current_close - spike_close) / spike_close * 100, 2),
        })

    return results


def _find_closest_close(
    target_ts: int,
    timestamps: list[int],
    ts_to_close: dict[int, float],
) -> Optional[float]:
    """Find close at target timestamp, or nearest within 3 trading days."""
    if target_ts in ts_to_close:
        return ts_to_close[target_ts]

    three_days = 3 * 86400
    matches = [
        (abs(ts - target_ts), ts)
        for ts in timestamps
        if abs(ts - target_ts) <= three_days
    ]
    
    if not matches:
        return None
        
    _, best_ts = min(matches)
    return ts_to_close.get(best_ts)
