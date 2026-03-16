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
        gap = spike_indices[i] - spike_indices[i - 1]
        if gap <= gap_days + 1:
            current_cluster.append(spike_indices[i])
        else:
            clusters.append(current_cluster)
            current_cluster = [spike_indices[i]]
    clusters.append(current_cluster)

    spikes = []
    for cluster in clusters:
        peak_index = max(cluster, key=lambda idx: closes[idx])
        dt = datetime.fromtimestamp(timestamps[peak_index], tz=timezone.utc)
        spikes.append({
            "dateString": f"{dt.month}/{dt.day}/{dt.strftime('%y')}",
            "timestamp": timestamps[peak_index],
            "vixClose": round(closes[peak_index], 2),
        })

    return spikes


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
        spike_ts = spike["timestamp"]
        spike_close = _find_closest_close(spike_ts, timestamps, ts_to_close)
        if spike_close is None or spike_close == 0:
            continue

        pct_gain = round((current_close - spike_close) / spike_close * 100, 2)
        results.append({
            "dateString": spike["dateString"],
            "vixClose": spike["vixClose"],
            "spikeClose": round(spike_close, 2),
            "pctGain": pct_gain,
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

    best_ts = None
    best_diff = float("inf")
    three_days = 3 * 86400

    for ts in timestamps:
        diff = abs(ts - target_ts)
        if diff <= three_days and diff < best_diff:
            best_diff = diff
            best_ts = ts

    return ts_to_close.get(best_ts) if best_ts is not None else None
