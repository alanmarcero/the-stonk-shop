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
    week_below: list[dict] = field(default_factory=list)
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
_5Y_RETURN_SYMBOLS = {"SPY", "QQQ", "DIA", "IWM", "TMUS"}


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
            print(f"[worker] All {total_batches} batches complete. Aggregating.")
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
        monthly[(dt.year, dt.month)] = close
    return list(monthly.values())


def _aggregate_to_quarterly(closes: list[float], timestamps: list[int]) -> list[float]:
    """Take the last close per calendar quarter from weekly data."""
    if not closes:
        return []
    quarterly: dict[tuple[int, int], float] = {}
    for close, ts in zip(closes, timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        quarterly[(dt.year, (dt.month - 1) // 3 + 1)] = close
    return list(quarterly.values())


def _pct_diff(close: float, ema_value: float) -> float:
    if not ema_value or ema_value <= 0:
        return 0.0
    return round((close - ema_value) / ema_value * 100, 2)


def _above_entry(symbol: str, name: str, close: float, ema_value: float, count: int) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "close": close,
        "ema": round(ema_value, 4),
        "pctAbove": _pct_diff(close, ema_value),
        "count": count,
    }


def _below_entry(symbol: str, name: str, close: float, ema_value: float, count: int) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "close": close,
        "ema": round(ema_value, 4),
        "pctBelow": _pct_diff(ema_value, close),
        "count": count,
    }


def _crossover_entry(symbol: str, name: str, close: float, ema_value: float, periods_below: int, period_key: str) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "close": close,
        "ema": round(ema_value, 4),
        "pctAbove": _pct_diff(close, ema_value),
        period_key: periods_below,
    }


def _crossdown_entry(symbol: str, name: str, close: float, ema_value: float, periods_above: int, period_key: str) -> dict:
    return {
        "symbol": symbol,
        "name": name,
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

        weekly_result = yahoo.fetch_quarterly_candles(symbol)
        daily_result = yahoo.fetch_stats_candles(symbol)

        if weekly_result is None and daily_result is None:
            print(f"[worker] {symbol}: fetch failed")
            batch.errors.append({"symbol": symbol, "error": "Failed to fetch candles"})
            continue

        name = weekly_result[2] if weekly_result else daily_result[2]

        weekly = _process_timeframe(symbol, name, weekly_result, "weeksBelow", "weeksAbove", min_below=MIN_WEEKS_THRESHOLD)
        _append_timeframe_results(batch, weekly, "crossovers", "crossdowns", "week_below", "week_above")

        monthly = _process_timeframe(symbol, name, weekly_result, "monthsBelow", "monthsAbove", aggregate_fn=_aggregate_to_monthly)
        _append_timeframe_results(batch, monthly, "month_crossovers", "month_crossdowns", "month_below", "month_above")

        quarterly = _process_timeframe(symbol, name, weekly_result, "quartersBelow", "quartersAbove", aggregate_fn=_aggregate_to_quarterly)
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
                computed.update({"symbol": symbol, "name": name})
                _add_special_stats(symbol, computed, daily_result, weekly_result)
                batch.stats_data.append(computed)

    return batch


def _add_special_stats(symbol: str, computed: dict, daily_result: tuple, weekly_result: Optional[tuple]) -> None:
    if symbol == "VOO":
        election = stats.compute_return_since(daily_result[0], daily_result[1], 2024, 11, 5)
        if election is not None: computed["spxSinceElection"] = election
        inauguration = stats.compute_return_since(daily_result[0], daily_result[1], 2025, 1, 20)
        if inauguration is not None: computed["spxSinceInauguration"] = inauguration
    
    if symbol in _5Y_RETURN_SYMBOLS and weekly_result is not None:
        ret = stats.compute_return_since(weekly_result[0], weekly_result[1], 2021, 3, 28)
        if ret is not None: computed["return5Y"] = ret


def _process_timeframe(
    symbol: str,
    name: str,
    candle_result: Optional[tuple[list[float], list[int], str]],
    cross_up_key: str,
    cross_down_key: str,
    aggregate_fn=None,
    min_below: int = 0,
) -> dict[str, Optional[dict]]:
    """Process candles for a single timeframe (weekly/monthly/quarterly)."""
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
    res = empty.copy()

    if (p_below := ema.detect_weekly_crossover(closes)) is not None:
        res["crossover"] = _crossover_entry(symbol, name, last_close, ema_value, p_below, cross_up_key)

    if (p_above := ema.detect_weekly_crossdown(closes)) is not None:
        res["crossdown"] = _crossdown_entry(symbol, name, last_close, ema_value, p_above, cross_down_key)

    if (b_count := ema.count_periods_below(closes)) is not None and b_count >= min_below:
        res["below"] = _below_entry(symbol, name, last_close, ema_value, b_count)

    if (a_count := ema.count_periods_above(closes)) is not None:
        res["above"] = _above_entry(symbol, name, last_close, ema_value, a_count)

    return res


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
        "weekBelow": batch.week_below,
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
    ("weekBelow", "count"),
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
    ("-below", {"weekBelow"}),
    ("-above", {"weekAbove"}),
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
        return resp.get("KeyCount", 0) >= total_batches
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
    aggregated.update({"stats": [], "errorDetails": []})
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
    base = {
        "scanDate": scan_date,
        "scanTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbolsScanned": total_symbols,
        "errors": total_errors,
    }

    for suffix, keys in _RESULT_FILES:
        res = {**base, **{k: aggregated[k] for k in keys}}
        _put_json(bucket, f"results/latest{suffix}.json", res)
        if snapshot:
            _put_json(bucket, f"results/{scan_date}{suffix}.json", res)

    aggregated["errorDetails"].sort(key=lambda x: x.get("symbol", ""))
    _put_json(bucket, "results/latest-errors.json", {**base, "errorDetails": aggregated["errorDetails"]})

    aggregated["stats"].sort(key=lambda x: x.get("symbol", ""))
    misc = _compute_misc_stats(aggregated["stats"], len(aggregated["weekAbove"]), total_symbols)
    stats_result = {**base, "stats": aggregated["stats"], "misc": misc}
    _put_json(bucket, "results/latest-stats.json", stats_result)

    if snapshot:
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
    h_pcts = [s["highPct"] for s in all_stats if "highPct" in s]
    y_pcts = [s["ytdPct"] for s in all_stats if "ytdPct" in s]
    f_pes = [s["forwardPE"] for s in all_stats if "forwardPE" in s]

    misc: dict = {}

    if h_pcts and total > 0:
        misc["pctWithin5OfHigh"] = round(sum(1 for h in h_pcts if h >= -5) / total * 100, 1)

    if y_pcts and total > 0:
        misc["pctPositiveYTD"] = round(sum(1 for y in y_pcts if y >= 0) / total * 100, 1)
        misc["avgYTD"] = round(sum(y_pcts) / len(y_pcts), 2)

    if f_pes:
        misc["avgForwardPE"] = round(sum(f_pes) / len(f_pes), 2)
        sorted_pes = sorted(f_pes)
        mid = len(sorted_pes) // 2
        misc["medianForwardPE"] = round((sorted_pes[mid-1] + sorted_pes[mid]) / 2 if len(sorted_pes) % 2 == 0 else sorted_pes[mid], 2)

    if total_symbols > 0:
        misc["pctAbove5wkEMA"] = round(week_above_count / total_symbols * 100, 1)
        misc["pctBelow5wkEMA"] = round((total_symbols - week_above_count) / total_symbols * 100, 1)

    for key, field in [("pctAbove200dSMA", "pctSma200d"), ("pctAbove200wSMA", "pctSma200w")]:
        vals = [s[field] for s in all_stats if s.get(field) is not None]
        if vals:
            misc[key] = round(sum(1 for v in vals if v >= 0) / len(vals) * 100, 1)

    voo = next((s for s in all_stats if s.get("symbol") == "VOO"), None)
    if voo:
        for k in ("spxSinceElection", "spxSinceInauguration"):
            if k in voo: misc[k] = voo[k]

    for sym in ("SPY", "QQQ", "DIA", "IWM", "TMUS"):
        entry = next((s for s in all_stats if s.get("symbol") == sym), None)
        if entry and "return5Y" in entry:
            misc[f"{sym.lower()}5Y"] = entry["return5Y"]

    return misc


def _update_manifest(bucket: str, scan_date: str) -> None:
    manifest = _read_json(bucket, "results/manifest.json") or {"weeks": []}
    weeks: list[str] = manifest.get("weeks", [])

    if scan_date in weeks:
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
        except Exception as err:
            print(f"[worker] failed to delete s3://{bucket}/{key}: {err}")


def _read_json(bucket: str, key: str) -> Any:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except Exception as err:
        print(f"[worker] failed to read s3://{bucket}/{key}: {err}")
        return None


def _put_json(bucket: str, key: str, data: Any) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))


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
