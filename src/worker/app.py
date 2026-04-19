import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

# Dual import supports both Lambda (package) and direct test execution contexts.
try:
    from . import aggregator, ema, stats, storage, yahoo
except ImportError:
    import aggregator, ema, stats, storage, yahoo


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


RATE_LIMIT_DELAY = 1
MIN_WEEKS_THRESHOLD = 3
MAX_WEEKLY_SNAPSHOTS = 6
_5Y_RETURN_SYMBOLS = {"VOO", "QQQ", "DIA", "IWM", "TMUS"}


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
            _aggregate_and_finalize(bucket, run_id, total_batches, snapshot=snapshot)
            if snapshot:
                _invalidate_cache()

    return {"statusCode": 200}


def _ensure_one_candle_per_week(closes: list[float], timestamps: list[int]) -> tuple[list[float], list[int]]:
    """Group candles by ISO week and keep only the latest one per week."""
    if not timestamps:
        return closes, timestamps
    
    # Use dict to keep only the last candle seen for each (year, week)
    weeks: dict[tuple[int, int], tuple[float, int]] = {}
    for c, t in zip(closes, timestamps):
        iso = datetime.fromtimestamp(t, tz=timezone.utc).isocalendar()
        weeks[(iso[0], iso[1])] = (c, t)
    
    # Sort by timestamp to preserve chronological order
    sorted_items = sorted(weeks.values(), key=lambda x: x[1])
    return [x[0] for x in sorted_items], [x[1] for x in sorted_items]


def _aggregate_to_monthly(closes: list[float], timestamps: list[int]) -> list[float]:
    """Take the last close per calendar month from weekly data, excluding current month."""
    if not closes:
        return []

    now = datetime.now(timezone.utc)
    monthly = {
        (dt.year, dt.month): close
        for close, ts in zip(closes, timestamps)
        if (dt := datetime.fromtimestamp(ts, tz=timezone.utc)).year < now.year or dt.month < now.month
    }
    return [monthly[k] for k in sorted(monthly.keys())]


def _aggregate_to_quarterly(closes: list[float], timestamps: list[int]) -> list[float]:
    """Take the last close per calendar quarter from weekly data, excluding current quarter."""
    if not closes:
        return []

    now = datetime.now(timezone.utc)
    now_q = (now.month - 1) // 3 + 1
    quarterly = {
        (dt.year, (dt.month - 1) // 3 + 1): close
        for close, ts in zip(closes, timestamps)
        if (dt := datetime.fromtimestamp(ts, tz=timezone.utc)).year < now.year or (dt.month - 1) // 3 + 1 < now_q
    }
    return [quarterly[k] for k in sorted(quarterly.keys())]


def _pct_diff(close: float, ema_value: float) -> float:
    if not ema_value or ema_value <= 0:
        return 0.0
    return round((close - ema_value) / ema_value * 100, 2)


def _entry(symbol: str, name: str, close: float, ema_val: float, val: int, key: str, is_above: bool) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "close": close,
        "ema": round(ema_val, 4),
        "pctAbove" if is_above else "pctBelow": _pct_diff(close, ema_val) if is_above else _pct_diff(ema_val, close),
        key: val,
    }


def _process_batch(symbols: list[dict], vix_spikes: Optional[list[dict]] = None) -> BatchResult:
    batch = BatchResult()
    for i, symbol_data in enumerate(symbols):
        if i > 0:
            time.sleep(RATE_LIMIT_DELAY)
        _process_symbol(symbol_data, batch, vix_spikes)
    return batch


def _append_timeframe_results(batch: BatchResult, timeframe_result: dict) -> None:
    """Helper to append individual timeframe results to the batch collections."""
    if crossover := timeframe_result.get("crossover"):
        batch.crossovers.append(crossover)
    if crossdown := timeframe_result.get("crossdown"):
        batch.crossdowns.append(crossdown)
    if below := timeframe_result.get("below"):
        batch.week_below.append(below)
    if above := timeframe_result.get("above"):
        batch.week_above.append(above)


def _append_monthly_results(batch: BatchResult, res: dict) -> None:
    if res.get("crossover"): batch.month_crossovers.append(res["crossover"])
    if res.get("crossdown"): batch.month_crossdowns.append(res["crossdown"])
    if res.get("below"): batch.month_below.append(res["below"])
    if res.get("above"): batch.month_above.append(res["above"])


def _append_quarterly_results(batch: BatchResult, res: dict) -> None:
    if res.get("crossover"): batch.quarter_crossovers.append(res["crossover"])
    if res.get("crossdown"): batch.quarter_crossdowns.append(res["crossdown"])
    if res.get("below"): batch.quarter_below.append(res["below"])
    if res.get("above"): batch.quarter_above.append(res["above"])


def _process_symbol(symbol_data: dict, batch: BatchResult, vix_spikes: Optional[list[dict]] = None) -> None:
    symbol = symbol_data["symbol"]
    market_cap = symbol_data.get("marketCap", 0)
    
    weekly_result = yahoo.fetch_quarterly_candles(symbol)
    daily_result = yahoo.fetch_stats_candles(symbol)

    if weekly_result is None and daily_result is None:
        print(f"[worker] {symbol}: fetch failed")
        batch.errors.append({"symbol": symbol, "error": "Failed to fetch candles"})
        return

    name = weekly_result[2] if weekly_result else daily_result[2]

    weekly = _process_timeframe(symbol, name, weekly_result, "weeksBelow", "weeksAbove", min_below=MIN_WEEKS_THRESHOLD)
    _append_timeframe_results(batch, weekly)

    monthly = _process_timeframe(symbol, name, weekly_result, "monthsBelow", "monthsAbove", aggregate_fn=_aggregate_to_monthly)
    _append_monthly_results(batch, monthly)

    quarterly = _process_timeframe(symbol, name, weekly_result, "quartersBelow", "quartersAbove", aggregate_fn=_aggregate_to_quarterly)
    _append_quarterly_results(batch, quarterly)

    if daily_result is not None:
        _process_stats_for_symbol(symbol, name, market_cap, daily_result, weekly_result, batch, weekly, monthly, quarterly, vix_spikes)


def _process_stats_for_symbol(
    symbol: str,
    name: str,
    market_cap: int,
    daily_result: tuple,
    weekly_result: Optional[tuple],
    batch: BatchResult,
    weekly: dict,
    monthly: dict,
    quarterly: dict,
    vix_spikes: Optional[list[dict]],
) -> None:
    forward_pe, pe_hist = yahoo.fetch_forward_pe(symbol)
    w_closes = weekly_result[0] if weekly_result is not None else None
    computed = stats.compute_stats(daily_result[0], daily_result[1], vix_spikes=vix_spikes, forward_pe=forward_pe, forward_pe_history=pe_hist, weekly_closes=w_closes)
    
    if computed is None:
        return

    computed.update({
        "symbol": symbol,
        "name": name,
        "marketCap": market_cap,
        "emaStatus": {
            "weekly": weekly.get("status"),
            "monthly": monthly.get("status"),
            "quarterly": quarterly.get("status")
        }
    })
    _add_special_stats(symbol, computed, daily_result, weekly_result)
    batch.stats_data.append(computed)


def _add_special_stats(symbol: str, computed: dict, daily_result: tuple, weekly_result: Optional[tuple]) -> None:
    if symbol == "VOO":
        # SPX proxy stats
        election = stats.compute_return_since(daily_result[0], daily_result[1], 2024, 11, 5)
        if election is not None: computed["spxSinceElection"] = election
        
        inauguration = stats.compute_return_since(daily_result[0], daily_result[1], 2025, 1, 20)
        if inauguration is not None: computed["spxSinceInauguration"] = inauguration

        chatgpt = stats.compute_return_since(daily_result[0], daily_result[1], 2022, 11, 30)
        if chatgpt is not None: computed["spxSinceChatGPT"] = chatgpt

        bottom2022 = stats.compute_return_since(daily_result[0], daily_result[1], 2022, 10, 12)
        if bottom2022 is not None: computed["spxSinceBottom2022"] = bottom2022


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

    # Ensure we have exactly one candle per period (week/month/quarter)
    # 1. Start with raw weekly candles and deduplicate by week
    closes, timestamps = _ensure_one_candle_per_week(candle_result[0], candle_result[1])
    
    # 2. If aggregating further (monthly/quarterly), exclude the current in-progress period
    if aggregate_fn is not None:
        closes = aggregate_fn(closes, timestamps)

    ema_value = ema.calculate(closes)
    if ema_value is None:
        return empty

    last_close = closes[-1]
    res = empty.copy()

    if (p_below := ema.detect_weekly_crossover(closes)) is not None:
        res["crossover"] = _entry(symbol, name, last_close, ema_value, p_below, cross_up_key, True)

    if (p_above := ema.detect_weekly_crossdown(closes)) is not None:
        res["crossdown"] = _entry(symbol, name, last_close, ema_value, p_above, cross_down_key, False)

    if (b_count := ema.count_periods_below(closes)) is not None and b_count >= min_below:
        res["below"] = _entry(symbol, name, last_close, ema_value, b_count, "count", False)

    if (a_count := ema.count_periods_above(closes)) is not None:
        res["above"] = _entry(symbol, name, last_close, ema_value, a_count, "count", True)

    res["status"] = {
        "above": last_close > ema_value,
        "count": a_count or b_count or 0,
        "pctDiff": _pct_diff(last_close, ema_value)
    }
    return res


def _write_batch_results(bucket: str, run_id: str, idx: int, count: int, err_count: int, batch: BatchResult) -> None:
    body = {
        "batchIndex": idx, "symbolsProcessed": count, "errors": err_count, "errorDetails": batch.errors,
        "crossovers": batch.crossovers, "crossdowns": batch.crossdowns, "weekBelow": batch.week_below, "weekAbove": batch.week_above,
        "monthCrossovers": batch.month_crossovers, "monthCrossdowns": batch.month_crossdowns, "monthBelow": batch.month_below, "monthAbove": batch.month_above,
        "quarterCrossovers": batch.quarter_crossovers, "quarterCrossdowns": batch.quarter_crossdowns, "quarterBelow": batch.quarter_below, "quarterAbove": batch.quarter_above,
        "stats": batch.stats_data,
    }
    storage.put_json(bucket, f"batches/{run_id}/batch-{idx:03d}.json", body)


def _write_errors(bucket: str, run_id: str, batch_index: int, errors: list[dict]) -> None:
    storage.put_json(bucket, f"logs/{run_id}/errors-{batch_index:03d}.json", errors)


def _all_batches_complete(bucket: str, run_id: str, total_batches: int) -> bool:
    """Check if all batch result files exist in S3."""
    return len(storage.list_objects(bucket, f"batches/{run_id}/")) >= total_batches


def _aggregate_and_finalize(bucket: str, run_id: str, total: int, *, snapshot: bool = False) -> None:
    # Use a lock file to ensure aggregation only happens once per unique run_id
    lock_key = f"locks/{run_id}.lock"
    if storage.object_exists(bucket, lock_key):
        return
    
    try:
        # Note: head_object + put_object is not atomic, but reduces races significantly 
        # in this low-concurrency environment (max 5 workers).
        storage.put_json(bucket, lock_key, {"triggeredAt": datetime.now(timezone.utc).isoformat()})
    except:
        return

    batch_data = [storage.read_json(bucket, f"batches/{run_id}/batch-{i:03d}.json") for i in range(total)]
    aggregated, total_symbols, total_errors = aggregator.aggregate_batches(batch_data)
    _write_results(bucket, aggregated, total_symbols, total_errors, snapshot=snapshot)


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


def _write_results(bucket: str, agg: dict, total_sym: int, total_err: int, *, snapshot: bool = False) -> None:
    now = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y-%m-%d")
    base = {"scanDate": scan_date, "scanTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "symbolsScanned": total_sym, "errors": total_err}

    for suffix, keys in _RESULT_FILES:
        res = {**base, **{k: agg[k] for k in keys}}
        storage.put_json(bucket, f"results/latest{suffix}.json", res)
        if snapshot:
            storage.put_json(bucket, f"results/{scan_date}{suffix}.json", res)

    _write_error_details(bucket, base, agg["errorDetails"])
    _write_stats_results(bucket, base, agg, total_sym, snapshot, scan_date)


def _write_error_details(bucket: str, base: dict, error_details: list[dict]) -> None:
    error_details.sort(key=lambda x: x.get("symbol", ""))
    storage.put_json(bucket, "results/latest-errors.json", {**base, "errorDetails": error_details})


def _write_stats_results(bucket: str, base: dict, agg: dict, total_sym: int, snapshot: bool, scan_date: str) -> None:
    stats_list = agg["stats"]
    stats_list.sort(key=lambda x: x.get("symbol", ""))
    misc = _compute_misc_stats(stats_list, len(agg["weekAbove"]), total_sym)
    stats_res = {**base, "stats": stats_list, "misc": misc}
    storage.put_json(bucket, "results/latest-stats.json", stats_res)

    if snapshot:
        storage.put_json(bucket, f"results/{scan_date}-stats.json", stats_res)
        _update_manifest(bucket, scan_date)


def _compute_misc_stats(all_stats: list[dict], week_above_count: int = 0, total_symbols: int = 0) -> dict:
    if not all_stats:
        return {}

    misc: dict = {}
    total = len(all_stats)

    _add_performance_averages(misc, all_stats, total)
    _add_ema_breadth_stats(misc, week_above_count, total_symbols)
    _add_sma_breadth_stats(misc, all_stats)
    _add_index_benchmarks(misc, all_stats)

    return misc


def _add_performance_averages(misc: dict, all_stats: list[dict], total: int) -> None:
    h_pcts = [s["highPct"] for s in all_stats if "highPct" in s]
    if h_pcts:
        misc["pctWithin5OfHigh"] = round(sum(1 for h in h_pcts if h >= -5) / total * 100, 1)

    y_pcts = [s["ytdPct"] for s in all_stats if "ytdPct" in s]
    if y_pcts:
        misc["pctPositiveYTD"] = round(sum(1 for y in y_pcts if y >= 0) / total * 100, 1)
        misc["avgYTD"] = round(sum(y_pcts) / len(y_pcts), 2)

    f_pes = [s["forwardPE"] for s in all_stats if "forwardPE" in s]
    if f_pes:
        misc["avgForwardPE"] = round(sum(f_pes) / len(f_pes), 2)
        sorted_pes = sorted(f_pes)
        mid = len(sorted_pes) // 2
        misc["medianForwardPE"] = round(
            (sorted_pes[mid - 1] + sorted_pes[mid]) / 2 if len(sorted_pes) % 2 == 0 else sorted_pes[mid], 2
        )


def _add_ema_breadth_stats(misc: dict, week_above_count: int, total_symbols: int) -> None:
    if total_symbols:
        misc["pctAbove5wkEMA"] = round(week_above_count / total_symbols * 100, 1)
        misc["pctBelow5wkEMA"] = round((total_symbols - week_above_count) / total_symbols * 100, 1)


def _add_sma_breadth_stats(misc: dict, all_stats: list[dict]) -> None:
    for key, field in [("pctAbove200dSMA", "pctSma200d"), ("pctAbove200wSMA", "pctSma200w")]:
        vals = [s[field] for s in all_stats if s.get(field) is not None]
        if vals:
            misc[key] = round(sum(1 for v in vals if v >= 0) / len(vals) * 100, 1)


def _add_index_benchmarks(misc: dict, all_stats: list[dict]) -> None:
    # SPX benchmarks from VOO
    voo = next((s for s in all_stats if s.get("symbol") == "VOO"), None)
    if voo:
        for k in ("spxSinceElection", "spxSinceInauguration", "spxSinceChatGPT", "spxSinceBottom2022"):
            if k in voo:
                misc[k] = voo[k]
        if "return5Y" in voo:
            misc["spx5Y"] = voo["return5Y"]

    # Benchmark tickers to consolidate
    bench_tickers = ("QQQ", "DIA", "IWM", "TMUS", "VTV", "SPY", "XLE", "XLK", "XLB", "XLV", "XLY", "XLI", "XLC", "XLU", "XLF", "XLRE", "XLP")
    bench_data = {}

    for sym in bench_tickers:
        entry = next((s for s in all_stats if s.get("symbol") == sym), None)
        if entry:
            ticker_stats = {
                "ytd": entry.get("ytdPct"),
                "oneY": entry.get("return1Y"),
                "fiveY": entry.get("return5Y")
            }
            bench_data[sym] = ticker_stats
            
            # Legacy fields for backward compat
            if ticker_stats["fiveY"] is not None:
                misc[f"{sym.lower()}5Y"] = ticker_stats["fiveY"]

    misc["benchmarkByTicker"] = bench_data



def _update_manifest(bucket: str, scan_date: str) -> None:
    manifest = storage.read_json(bucket, "results/manifest.json") or {"weeks": []}
    weeks: list[str] = manifest.get("weeks", [])
    if scan_date in weeks: weeks.remove(scan_date)
    weeks.insert(0, scan_date)
    
    trimmed = weeks[MAX_WEEKLY_SNAPSHOTS:]
    weeks = weeks[:MAX_WEEKLY_SNAPSHOTS]

    for old_date in trimmed:
        _delete_snapshot(bucket, old_date)

    storage.put_json(bucket, "results/manifest.json", {"weeks": weeks})


def _delete_snapshot(bucket: str, scan_date: str) -> None:
    suffixes = ["", "-crossdown", "-below", "-above", "-monthly", "-monthly-below-above", "-quarterly", "-quarterly-below-above", "-stats"]
    for suffix in suffixes:
        storage.delete_object(bucket, f"results/{scan_date}{suffix}.json")


def _invalidate_cache() -> None:
    dist_id = os.environ.get("DISTRIBUTION_ID")
    if dist_id:
        storage.invalidate_cache(dist_id, ["/results/*"])
        print(f"[worker] CloudFront invalidation created for {dist_id}")
