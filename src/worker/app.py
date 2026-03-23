import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import boto3

# Dual import supports both Lambda (package) and direct test execution contexts.
try:
    from . import ema, stats, yahoo
except ImportError:
    import ema, stats, yahoo


@dataclass
class BatchResult:
    """Results from processing a batch of symbols through EMA analysis."""

    crossovers: list[dict] = field(default_factory=list)
    crossdowns: list[dict] = field(default_factory=list)
    day_below: list[dict] = field(default_factory=list)
    week_below: list[dict] = field(default_factory=list)
    day_above: list[dict] = field(default_factory=list)
    week_above: list[dict] = field(default_factory=list)
    month_crossovers: list[dict] = field(default_factory=list)
    month_crossdowns: list[dict] = field(default_factory=list)
    month_below: list[dict] = field(default_factory=list)
    month_above: list[dict] = field(default_factory=list)
    quarter_crossovers: list[dict] = field(default_factory=list)
    quarter_crossdowns: list[dict] = field(default_factory=list)
    quarter_below: list[dict] = field(default_factory=list)
    quarter_above: list[dict] = field(default_factory=list)
    stats_data: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)

s3 = boto3.client("s3")
cloudfront = boto3.client("cloudfront")

RATE_LIMIT_DELAY = 1
MIN_WEEKS_THRESHOLD = 3
MAX_WEEKLY_SNAPSHOTS = 6


def lambda_handler(event: dict, context: Any) -> dict:
    bucket = os.environ["BUCKET_NAME"]

    for record in event.get("Records", []):
        message = json.loads(record["body"])
        run_id: str = message["runId"]
        batch_index: int = message["batchIndex"]
        total_batches: int = message["totalBatches"]
        symbols: list[str] = message["symbols"]
        vix_spikes: list[dict] = message.get("vixSpikes", [])
        snapshot: bool = message.get("snapshot", False)

        result = _process_batch(symbols, vix_spikes)

        _write_batch_results(bucket, run_id, batch_index, len(symbols), len(result.errors), result)

        if result.errors:
            _write_errors(bucket, run_id, batch_index, result.errors)

        if _all_batches_complete(bucket, run_id, total_batches):
            _aggregate_results(bucket, run_id, total_batches, snapshot=snapshot)
            _invalidate_cache()

    return {"statusCode": 200}


def _strip_incomplete_week(closes: list[float], timestamps: list[int]) -> tuple[list[float], list[int]]:
    """Drop the last candle if it belongs to the current (incomplete) week."""
    if not timestamps:
        return closes, timestamps
    now = datetime.now(timezone.utc)
    last_dt = datetime.fromtimestamp(timestamps[-1], tz=timezone.utc)
    if last_dt.isocalendar()[1] == now.isocalendar()[1] and last_dt.year == now.year:
        return closes[:-1], timestamps[:-1]
    return closes, timestamps


def _aggregate_to_monthly(closes: list[float], timestamps: list[int]) -> list[float]:
    """Take the last close per calendar month from weekly data."""
    if not closes:
        return []
    monthly: dict[tuple[int, int], float] = {}
    for close, ts in zip(closes, timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        key = (dt.year, dt.month)
        monthly[key] = close
    return list(monthly.values())


def _aggregate_to_quarterly(closes: list[float], timestamps: list[int]) -> list[float]:
    """Take the last close per calendar quarter from weekly data."""
    if not closes:
        return []
    quarterly: dict[tuple[int, int], float] = {}
    for close, ts in zip(closes, timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        quarter = (dt.month - 1) // 3 + 1
        key = (dt.year, quarter)
        quarterly[key] = close
    return list(quarterly.values())


def _pct_diff(close: float, ema_value: float) -> float:
    return round((close - ema_value) / ema_value * 100, 2)


def _above_entry(symbol: str, close: float, ema_value: float, count: int) -> dict:
    return {
        "symbol": symbol,
        "close": close,
        "ema": round(ema_value, 4),
        "pctAbove": _pct_diff(close, ema_value),
        "count": count,
    }


def _below_entry(symbol: str, close: float, ema_value: float, count: int) -> dict:
    return {
        "symbol": symbol,
        "close": close,
        "ema": round(ema_value, 4),
        "pctBelow": _pct_diff(ema_value, close),
        "count": count,
    }


def _crossover_entry(symbol: str, close: float, ema_value: float, periods_below: int, period_key: str) -> dict:
    return {
        "symbol": symbol,
        "close": close,
        "ema": round(ema_value, 4),
        "pctAbove": _pct_diff(close, ema_value),
        period_key: periods_below,
    }


def _crossdown_entry(symbol: str, close: float, ema_value: float, periods_above: int, period_key: str) -> dict:
    return {
        "symbol": symbol,
        "close": close,
        "ema": round(ema_value, 4),
        "pctBelow": _pct_diff(ema_value, close),
        period_key: periods_above,
    }


def _append_timeframe_results(
    batch: BatchResult,
    result: dict[str, Optional[dict]],
    cross_up_attr: str,
    cross_down_attr: str,
    below_attr: str,
    above_attr: str,
) -> None:
    if result["crossover"] is not None:
        getattr(batch, cross_up_attr).append(result["crossover"])
    if result["crossdown"] is not None:
        getattr(batch, cross_down_attr).append(result["crossdown"])
    if result["below"] is not None:
        getattr(batch, below_attr).append(result["below"])
    if result["above"] is not None:
        getattr(batch, above_attr).append(result["above"])


def _process_batch(
    symbols: list[str],
    vix_spikes: Optional[list[dict]] = None,
) -> BatchResult:
    batch = BatchResult()

    for i, symbol in enumerate(symbols):
        if i > 0:
            time.sleep(RATE_LIMIT_DELAY)

        # Two API calls instead of four: 5y weekly covers weekly/monthly/quarterly
        # EMA analysis, 3y daily covers daily EMA and all stats computations.
        weekly_result = yahoo.fetch_quarterly_candles(symbol)
        daily_result = yahoo.fetch_stats_candles(symbol)

        if weekly_result is None and daily_result is None:
            print(f"[worker] {symbol}: fetch failed")
            batch.errors.append({"symbol": symbol, "error": "Failed to fetch candles"})
            continue

        daily_above, daily_below = _process_daily(symbol, daily_result)
        if daily_above is not None:
            batch.day_above.append(daily_above)
        if daily_below is not None:
            batch.day_below.append(daily_below)

        weekly = _process_timeframe(
            symbol, weekly_result, "weeksBelow", "weeksAbove",
            min_below=MIN_WEEKS_THRESHOLD,
        )
        monthly = _process_timeframe(
            symbol, weekly_result, "monthsBelow", "monthsAbove",
            aggregate_fn=_aggregate_to_monthly,
        )
        quarterly = _process_timeframe(
            symbol, weekly_result, "quartersBelow", "quartersAbove",
            aggregate_fn=_aggregate_to_quarterly,
        )

        _append_timeframe_results(batch, weekly, "crossovers", "crossdowns", "week_below", "week_above")
        _append_timeframe_results(batch, monthly, "month_crossovers", "month_crossdowns", "month_below", "month_above")
        _append_timeframe_results(batch, quarterly, "quarter_crossovers", "quarter_crossdowns", "quarter_below", "quarter_above")

        if daily_result is not None:
            forward_pe, pe_history = yahoo.fetch_forward_pe(symbol)
            q_closes = weekly_result[0] if weekly_result is not None else None
            computed = stats.compute_stats(
                daily_result[0], daily_result[1],
                vix_spikes=vix_spikes,
                forward_pe=forward_pe,
                forward_pe_history=pe_history,
                weekly_closes=q_closes,
            )
            if computed is not None:
                computed["symbol"] = symbol
                if symbol == "VOO":
                    closes, timestamps = daily_result
                    election = stats.compute_return_since(closes, timestamps, 2024, 11, 5)
                    if election is not None:
                        computed["spxSinceElection"] = election
                    inauguration = stats.compute_return_since(closes, timestamps, 2025, 1, 20)
                    if inauguration is not None:
                        computed["spxSinceInauguration"] = inauguration
                batch.stats_data.append(computed)

    return batch


def _process_daily(
    symbol: str,
    daily_result: Optional[tuple[list[float], list[int]]],
) -> tuple[Optional[dict], Optional[dict]]:
    """Process daily candles for a symbol.

    Returns (day_above_entry, day_below_entry), either may be None.
    """
    if daily_result is None:
        return None, None
    daily_closes = daily_result[0]
    daily_ema_value = ema.calculate(daily_closes)
    if daily_ema_value is None:
        return None, None
    last_close = daily_closes[-1]

    above_entry = None
    above_count = ema.count_periods_above(daily_closes)
    if above_count is not None:
        above_entry = _above_entry(symbol, last_close, daily_ema_value, above_count)

    below_entry = None
    below_count = ema.count_periods_below(daily_closes)
    if below_count is not None:
        below_entry = _below_entry(symbol, last_close, daily_ema_value, below_count)

    return above_entry, below_entry


def _process_timeframe(
    symbol: str,
    candle_result: Optional[tuple[list[float], list[int]]],
    cross_up_key: str,
    cross_down_key: str,
    aggregate_fn=None,
    min_below: int = 0,
) -> dict[str, Optional[dict]]:
    """Process candles for a single timeframe (weekly/monthly/quarterly).

    Returns dict with keys: crossover, crossdown, below, above (each Optional[dict]).
    """
    empty = {"crossover": None, "crossdown": None, "below": None, "above": None}
    if candle_result is None:
        return empty

    closes, timestamps = _strip_incomplete_week(candle_result[0], candle_result[1])
    if aggregate_fn is not None:
        closes = aggregate_fn(closes, timestamps)
    ema_value = ema.calculate(closes)
    if ema_value is None:
        return empty
    last_close = closes[-1]

    crossover = None
    periods_below = ema.detect_weekly_crossover(closes)
    if periods_below is not None:
        crossover = _crossover_entry(symbol, last_close, ema_value, periods_below, cross_up_key)

    crossdown = None
    periods_above = ema.detect_weekly_crossdown(closes)
    if periods_above is not None:
        crossdown = _crossdown_entry(symbol, last_close, ema_value, periods_above, cross_down_key)

    below = None
    below_count = ema.count_periods_below(closes)
    if below_count is not None and below_count >= min_below:
        below = _below_entry(symbol, last_close, ema_value, below_count)

    above = None
    above_count = ema.count_periods_above(closes)
    if above_count is not None:
        above = _above_entry(symbol, last_close, ema_value, above_count)

    return {"crossover": crossover, "crossdown": crossdown, "below": below, "above": above}


def _write_batch_results(
    bucket: str,
    run_id: str,
    batch_index: int,
    symbol_count: int,
    error_count: int,
    batch: BatchResult,
) -> None:
    body = {
        "batchIndex": batch_index,
        "symbolsProcessed": symbol_count,
        "errors": error_count,
        "errorDetails": batch.errors,
        "crossovers": batch.crossovers,
        "crossdowns": batch.crossdowns,
        "dayBelow": batch.day_below,
        "weekBelow": batch.week_below,
        "dayAbove": batch.day_above,
        "weekAbove": batch.week_above,
        "monthCrossovers": batch.month_crossovers,
        "monthCrossdowns": batch.month_crossdowns,
        "monthBelow": batch.month_below,
        "monthAbove": batch.month_above,
        "quarterCrossovers": batch.quarter_crossovers,
        "quarterCrossdowns": batch.quarter_crossdowns,
        "quarterBelow": batch.quarter_below,
        "quarterAbove": batch.quarter_above,
        "stats": batch.stats_data,
    }
    key = f"batches/{run_id}/batch-{batch_index:03d}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(body))


def _write_errors(bucket: str, run_id: str, batch_index: int, errors: list[dict]) -> None:
    key = f"logs/{run_id}/errors-{batch_index:03d}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(errors))


_AGGREGATE_KEYS = [
    ("crossovers", "weeksBelow"),
    ("crossdowns", "weeksAbove"),
    ("dayBelow", "count"),
    ("weekBelow", "count"),
    ("dayAbove", "count"),
    ("weekAbove", "count"),
    ("monthCrossovers", "monthsBelow"),
    ("monthCrossdowns", "monthsAbove"),
    ("monthBelow", "count"),
    ("monthAbove", "count"),
    ("quarterCrossovers", "quartersBelow"),
    ("quarterCrossdowns", "quartersAbove"),
    ("quarterBelow", "count"),
    ("quarterAbove", "count"),
]

_RESULT_FILES = [
    ("", {"crossovers"}),
    ("-crossdown", {"crossdowns"}),
    ("-below", {"dayBelow", "weekBelow"}),
    ("-above", {"dayAbove", "weekAbove"}),
    ("-monthly", {"monthCrossovers", "monthCrossdowns"}),
    ("-monthly-below-above", {"monthBelow", "monthAbove"}),
    ("-quarterly", {"quarterCrossovers", "quarterCrossdowns"}),
    ("-quarterly-below-above", {"quarterBelow", "quarterAbove"}),
]


def _all_batches_complete(bucket: str, run_id: str, total_batches: int) -> bool:
    """Check if all batch result files exist in S3."""
    prefix = f"batches/{run_id}/"
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        count = resp.get("KeyCount", 0)
        return count >= total_batches
    except Exception as err:
        print(f"[worker] failed to list batches: {err}")
        return False


def _aggregate_results(bucket: str, run_id: str, total_batches: int, *, snapshot: bool = False) -> None:
    aggregated, total_symbols, total_errors = _read_batches(bucket, run_id, total_batches)
    _sort_aggregated(aggregated)
    _write_results(bucket, aggregated, total_symbols, total_errors, snapshot=snapshot)


def _read_batches(
    bucket: str, run_id: str, total_batches: int,
) -> tuple[dict[str, list[dict]], int, int]:
    aggregated: dict[str, list[dict]] = {key: [] for key, _ in _AGGREGATE_KEYS}
    aggregated["stats"] = []
    aggregated["errorDetails"] = []
    total_symbols = 0
    total_errors = 0

    for i in range(total_batches):
        key = f"batches/{run_id}/batch-{i:03d}.json"
        batch = _read_json(bucket, key)
        if batch is None:
            continue
        for json_key, _ in _AGGREGATE_KEYS:
            aggregated[json_key].extend(batch.get(json_key, []))
        aggregated["stats"].extend(batch.get("stats", []))
        aggregated["errorDetails"].extend(batch.get("errorDetails", []))
        total_symbols += batch.get("symbolsProcessed", 0)
        total_errors += batch.get("errors", 0)

    return aggregated, total_symbols, total_errors


def _sort_aggregated(aggregated: dict[str, list[dict]]) -> None:
    for json_key, sort_field in _AGGREGATE_KEYS:
        aggregated[json_key].sort(key=lambda x, f=sort_field: x.get(f, 0), reverse=True)


def _write_results(
    bucket: str, aggregated: dict[str, list[dict]], total_symbols: int, total_errors: int,
    *, snapshot: bool = False,
) -> None:
    now = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y-%m-%d")
    scan_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    base = {
        "scanDate": scan_date,
        "scanTime": scan_time,
        "symbolsScanned": total_symbols,
        "errors": total_errors,
    }

    results_by_suffix = {}
    for suffix, keys in _RESULT_FILES:
        results_by_suffix[suffix] = {**base, **{k: aggregated[k] for k in keys}}

    for suffix, _ in _RESULT_FILES:
        _put_json(bucket, f"results/latest{suffix}.json", results_by_suffix[suffix])

    aggregated["errorDetails"].sort(key=lambda x: x.get("symbol", ""))
    _put_json(bucket, "results/latest-errors.json", {**base, "errorDetails": aggregated["errorDetails"]})

    aggregated["stats"].sort(key=lambda x: x.get("symbol", ""))
    misc = _compute_misc_stats(aggregated["stats"], len(aggregated["weekAbove"]), total_symbols)
    stats_result = {**base, "stats": aggregated["stats"], "misc": misc}
    _put_json(bucket, "results/latest-stats.json", stats_result)

    if snapshot:
        for suffix, _ in _RESULT_FILES:
            _put_json(bucket, f"results/{scan_date}{suffix}.json", results_by_suffix[suffix])
        _put_json(bucket, f"results/{scan_date}-stats.json", stats_result)
        _update_manifest(bucket, scan_date)


def _compute_misc_stats(
    all_stats: list[dict],
    week_above_count: int = 0,
    total_symbols: int = 0,
) -> dict:
    """Compute aggregate misc stats from all symbol stats."""
    if not all_stats:
        return {}

    total = len(all_stats)

    high_pcts = [s["highPct"] for s in all_stats if "highPct" in s]
    ytd_pcts = [s["ytdPct"] for s in all_stats if "ytdPct" in s]
    forward_pes = [s["forwardPE"] for s in all_stats if "forwardPE" in s]

    misc: dict = {}

    if high_pcts:
        within_5 = sum(1 for h in high_pcts if h >= -5)
        misc["pctWithin5OfHigh"] = round(within_5 / total * 100, 1)

    if ytd_pcts:
        positive_ytd = sum(1 for y in ytd_pcts if y >= 0)
        misc["pctPositiveYTD"] = round(positive_ytd / total * 100, 1)
        misc["avgYTD"] = round(sum(ytd_pcts) / len(ytd_pcts), 2)

    if forward_pes:
        misc["avgForwardPE"] = round(sum(forward_pes) / len(forward_pes), 2)
        sorted_pes = sorted(forward_pes)
        mid = len(sorted_pes) // 2
        if len(sorted_pes) % 2 == 0:
            misc["medianForwardPE"] = round((sorted_pes[mid - 1] + sorted_pes[mid]) / 2, 2)
        else:
            misc["medianForwardPE"] = round(sorted_pes[mid], 2)

    # EMA above/below percentages
    if total_symbols > 0:
        misc["pctAbove5wkEMA"] = round(week_above_count / total_symbols * 100, 1)
        misc["pctBelow5wkEMA"] = round((total_symbols - week_above_count) / total_symbols * 100, 1)

    # SMA breadth
    above_200d = sum(1 for s in all_stats if s.get("pctSma200d") is not None and s["pctSma200d"] >= 0)
    has_200d = sum(1 for s in all_stats if s.get("pctSma200d") is not None)
    if has_200d > 0:
        misc["pctAbove200dSMA"] = round(above_200d / has_200d * 100, 1)

    above_200w = sum(1 for s in all_stats if s.get("pctSma200w") is not None and s["pctSma200w"] >= 0)
    has_200w = sum(1 for s in all_stats if s.get("pctSma200w") is not None)
    if has_200w > 0:
        misc["pctAbove200wSMA"] = round(above_200w / has_200w * 100, 1)

    # SPX milestone returns (from VOO)
    voo = next((s for s in all_stats if s.get("symbol") == "VOO"), None)
    if voo:
        if "spxSinceElection" in voo:
            misc["spxSinceElection"] = voo["spxSinceElection"]
        if "spxSinceInauguration" in voo:
            misc["spxSinceInauguration"] = voo["spxSinceInauguration"]

    return misc


def _update_manifest(bucket: str, scan_date: str) -> None:
    manifest = _read_json(bucket, "results/manifest.json") or {"weeks": []}
    weeks: list[str] = manifest.get("weeks", [])

    if scan_date not in weeks:
        weeks.insert(0, scan_date)
    else:
        weeks.remove(scan_date)
        weeks.insert(0, scan_date)

    trimmed = weeks[MAX_WEEKLY_SNAPSHOTS:]
    weeks = weeks[:MAX_WEEKLY_SNAPSHOTS]

    for old_date in trimmed:
        _delete_snapshot(bucket, old_date)

    _put_json(bucket, "results/manifest.json", {"weeks": weeks})


def _delete_snapshot(bucket: str, scan_date: str) -> None:
    suffixes = ["", "-crossdown", "-below", "-above", "-monthly", "-monthly-below-above", "-quarterly", "-quarterly-below-above", "-stats"]
    for suffix in suffixes:
        key = f"results/{scan_date}{suffix}.json"
        try:
            s3.delete_object(Bucket=bucket, Key=key)
        except Exception as err:  # Broad catch: S3 errors must not halt cleanup of other snapshots
            print(f"[worker] failed to delete s3://{bucket}/{key}: {err}")


def _read_json(bucket: str, key: str) -> Any:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except Exception as err:  # Broad catch: missing/corrupt batch files should not abort aggregation
        print(f"[worker] failed to read s3://{bucket}/{key}: {err}")
        return None


def _invalidate_cache() -> None:
    dist_id = os.environ.get("DISTRIBUTION_ID")
    if not dist_id:
        print("[worker] DISTRIBUTION_ID not set, skipping cache invalidation")
        return
    cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": ["/results/*"]},
            "CallerReference": datetime.now(timezone.utc).isoformat(),
        },
    )
    print(f"[worker] CloudFront invalidation created for {dist_id}")


def _put_json(bucket: str, key: str, data: Any) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))
