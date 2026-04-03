import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

try:
    from . import ema, stats, yahoo, storage
except ImportError:
    import ema, stats, yahoo, storage


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
_5Y_RETURN_SYMBOLS = {"SPY", "QQQ", "DIA", "IWM", "TMUS"}


def lambda_handler(event: dict, context: Any) -> dict:
    bucket = os.environ["BUCKET_NAME"]

    for record in event.get("Records", []):
        message = json.loads(record["body"])
        run_id, batch_idx, total_batches, symbols = message["runId"], message["batchIndex"], message["totalBatches"], message["symbols"]
        vix_spikes, snapshot = message.get("vixSpikes", []), message.get("snapshot", False)

        result = _process_batch(symbols, vix_spikes)
        _write_batch_results(bucket, run_id, batch_idx, len(symbols), len(result.errors), result)

        if result.errors:
            storage.put_json(bucket, f"logs/{run_id}/errors-{batch_idx:03d}.json", result.errors)

        if storage.all_batches_complete(bucket, run_id, total_batches):
            if storage.acquire_aggregation_lock(bucket, run_id):
                print(f"[worker] All {total_batches} batches complete. Aggregating.")
                _aggregate_and_finalize(bucket, run_id, total_batches, snapshot=snapshot)
                if snapshot:
                    _invalidate_cache()
            else:
                print(f"[worker] Aggregation already in progress or completed by another worker for run {run_id}")

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
    monthly: dict[tuple[int, int], float] = {}
    
    for close, ts in zip(closes, timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        # Only include if month is before current month OR it's a previous year
        if dt.year < now.year or dt.month < now.month:
            monthly[(dt.year, dt.month)] = close
            
    return [monthly[k] for k in sorted(monthly.keys())]


def _aggregate_to_quarterly(closes: list[float], timestamps: list[int]) -> list[float]:
    """Take the last close per calendar quarter from weekly data, excluding current quarter."""
    if not closes:
        return []
    
    now = datetime.now(timezone.utc)
    now_q = (now.month - 1) // 3 + 1
    quarterly: dict[tuple[int, int], float] = {}
    
    for close, ts in zip(closes, timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        q = (dt.month - 1) // 3 + 1
        # Only include if quarter is before current quarter OR it's a previous year
        if dt.year < now.year or q < now_q:
            quarterly[(dt.year, q)] = close
            
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


def _process_batch(
    symbols: list[str],
    vix_spikes: Optional[list[dict]] = None,
) -> BatchResult:
    batch = BatchResult()

    for i, symbol in enumerate(symbols):
        if i > 0:
            time.sleep(RATE_LIMIT_DELAY)

        # 5y weekly covers weekly/monthly/quarterly EMA analysis, 3y daily covers daily EMA and all stats.
        w_res = yahoo.fetch_quarterly_candles(symbol)
        d_res = yahoo.fetch_stats_candles(symbol)

        if not w_res and not d_res:
            print(f"[worker] {symbol}: fetch failed")
            batch.errors.append({"symbol": symbol, "error": "Failed to fetch candles"})
            continue

        name = w_res[2] if w_res else d_res[2]

        # Weekly/Monthly/Quarterly EMA
        for aggregate_fn, cross_up_attr, cross_down_attr, below_attr, above_attr, cross_up_key, cross_down_key, min_b in [
            (None, "crossovers", "crossdowns", "week_below", "week_above", "weeksBelow", "weeksAbove", MIN_WEEKS_THRESHOLD),
            (_aggregate_to_monthly, "month_crossovers", "month_crossdowns", "month_below", "month_above", "monthsBelow", "monthsAbove", 0),
            (_aggregate_to_quarterly, "quarter_crossovers", "quarter_crossdowns", "quarter_below", "quarter_above", "quartersBelow", "quartersAbove", 0),
        ]:
            if not w_res: continue
            closes, ts = _ensure_one_candle_per_week(w_res[0], w_res[1])
            if aggregate_fn: closes = aggregate_fn(closes, ts)
            
            ema_val = ema.calculate(closes)
            if ema_val is None: continue
            
            last_close = closes[-1]
            if (val := ema.detect_weekly_crossover(closes)) is not None:
                getattr(batch, cross_up_attr).append(_entry(symbol, name, last_close, ema_val, val, cross_up_key, True))
            if (val := ema.detect_weekly_crossdown(closes)) is not None:
                getattr(batch, cross_down_attr).append(_entry(symbol, name, last_close, ema_val, val, cross_down_key, False))
            if (val := ema.count_periods_below(closes)) is not None and val >= min_b:
                getattr(batch, below_attr).append(_entry(symbol, name, last_close, ema_val, val, "count", False))
            if (val := ema.count_periods_above(closes)) is not None:
                getattr(batch, above_attr).append(_entry(symbol, name, last_close, ema_val, val, "count", True))

        if d_res is not None:
            f_pe, pe_h = yahoo.fetch_forward_pe(symbol)
            computed = stats.compute_stats(d_res[0], d_res[1], vix_spikes=vix_spikes, forward_pe=f_pe, forward_pe_history=pe_h, weekly_closes=w_res[0] if w_res else None)
            if computed:
                computed.update({"symbol": symbol, "name": name})
                if symbol == "VOO":
                    for k, d in [("spxSinceElection", (2024, 11, 5)), ("spxSinceInauguration", (2025, 1, 20))]:
                        if (ret := stats.compute_return_since(d_res[0], d_res[1], *d)) is not None: computed[k] = ret
                if symbol == "SPY" and (ret := stats.compute_return_since(d_res[0], d_res[1], 2022, 11, 30)) is not None:
                    computed["spySinceChatGPT"] = ret
                if symbol in _5Y_RETURN_SYMBOLS and w_res:
                    if (ret := stats.compute_return_since(w_res[0], w_res[1], 2021, 3, 28)) is not None: computed["return5Y"] = ret
                batch.stats_data.append(computed)

    return batch


def _write_batch_results(bucket: str, run_id: str, batch_idx: int, sym_count: int, err_count: int, batch: BatchResult) -> None:
    body = {
        "batchIndex": batch_idx, "symbolsProcessed": sym_count, "errors": err_count, "errorDetails": batch.errors,
        "crossovers": batch.crossovers, "crossdowns": batch.crossdowns, "weekBelow": batch.week_below, "weekAbove": batch.week_above,
        "monthCrossovers": batch.month_crossovers, "monthCrossdowns": batch.month_crossdowns, "monthBelow": batch.month_below, "monthAbove": batch.month_above,
        "quarterCrossovers": batch.quarter_crossovers, "quarterCrossdowns": batch.quarter_crossdowns, "quarterBelow": batch.quarter_below, "quarterAbove": batch.quarter_above,
        "stats": batch.stats_data,
    }
    storage.put_json(bucket, f"batches/{run_id}/batch-{batch_idx:03d}.json", body)


_AGGREGATE_KEYS = [
    ("crossovers", "weeksBelow"), ("crossdowns", "weeksAbove"), ("weekBelow", "count"), ("weekAbove", "count"),
    ("monthCrossovers", "monthsBelow"), ("monthCrossdowns", "monthsAbove"), ("monthBelow", "count"), ("monthAbove", "count"),
    ("quarterCrossovers", "quartersBelow"), ("quarterCrossdowns", "quartersAbove"), ("quarterBelow", "count"), ("quarterAbove", "count"),
]

_RESULT_FILES = [
    ("", {"crossovers"}), ("-crossdown", {"crossdowns"}), ("-below", {"weekBelow"}), ("-above", {"weekAbove"}),
    ("-monthly", {"monthCrossovers", "monthCrossdowns"}), ("-monthly-below-above", {"monthBelow", "monthAbove"}),
    ("-quarterly", {"quarterCrossovers", "quarterCrossdowns"}), ("-quarterly-below-above", {"quarterBelow", "quarterAbove"}),
]


def _aggregate_and_finalize(bucket: str, run_id: str, total_batches: int, *, snapshot: bool = False) -> None:
    agg: dict[str, list[dict]] = {key: [] for key, _ in _AGGREGATE_KEYS}
    agg.update({"stats": [], "errorDetails": []})
    total_sym, total_err = 0, 0

    for i in range(total_batches):
        batch = storage.read_json(bucket, f"batches/{run_id}/batch-{i:03d}.json")
        if not batch: continue
        for k, _ in _AGGREGATE_KEYS: agg[k].extend(batch.get(k, []))
        agg["stats"].extend(batch.get("stats", []))
        agg["errorDetails"].extend(batch.get("errorDetails", []))
        total_sym += batch.get("symbolsProcessed", 0)
        total_err += batch.get("errors", 0)

    for k, f in _AGGREGATE_KEYS: agg[k].sort(key=lambda x: x.get(f, 0), reverse=True)
    _write_results(bucket, agg, total_sym, total_err, snapshot=snapshot)


def _write_results(bucket: str, agg: dict, total_sym: int, total_err: int, *, snapshot: bool = False) -> None:
    now = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y-%m-%d")
    base = {"scanDate": scan_date, "scanTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "symbolsScanned": total_sym, "errors": total_err}

    for suffix, keys in _RESULT_FILES:
        res = {**base, **{k: agg[k] for k in keys}}
        storage.put_json(bucket, f"results/latest{suffix}.json", res)
        if snapshot:
            storage.put_json(bucket, f"results/{scan_date}{suffix}.json", res)

    agg["errorDetails"].sort(key=lambda x: x.get("symbol", ""))
    storage.put_json(bucket, "results/latest-errors.json", {**base, "errorDetails": agg["errorDetails"]})

    agg["stats"].sort(key=lambda x: x.get("symbol", ""))
    misc = _compute_misc_stats(agg["stats"], len(agg["weekAbove"]), total_sym)
    stats_res = {**base, "stats": agg["stats"], "misc": misc}
    storage.put_json(bucket, "results/latest-stats.json", stats_res)
    if snapshot:
        storage.put_json(bucket, f"results/{scan_date}-stats.json", stats_res)
        _update_manifest(bucket, scan_date)


def _compute_misc_stats(all_stats: list[dict], week_above_count: int = 0, total_symbols: int = 0) -> dict:
    if not all_stats: return {}
    total = len(all_stats)
    h_pcts = [s["highPct"] for s in all_stats if "highPct" in s]
    y_pcts = [s["ytdPct"] for s in all_stats if "ytdPct" in s]
    f_pes = [s["forwardPE"] for s in all_stats if "forwardPE" in s]

    misc: dict = {}
    if h_pcts and total:
        misc["pctWithin5OfHigh"] = round(sum(1 for h in h_pcts if h >= -5) / total * 100, 1)
    if y_pcts and total:
        misc["pctPositiveYTD"] = round(sum(1 for y in y_pcts if y >= 0) / total * 100, 1)
        misc["avgYTD"] = round(sum(y_pcts) / len(y_pcts), 2)
    if f_pes:
        misc["avgForwardPE"] = round(sum(f_pes) / len(f_pes), 2)
        sorted_pes = sorted(f_pes)
        mid = len(sorted_pes) // 2
        misc["medianForwardPE"] = round((sorted_pes[mid-1] + sorted_pes[mid]) / 2 if len(sorted_pes) % 2 == 0 else sorted_pes[mid], 2)

    if total_symbols:
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

    spy = next((s for s in all_stats if s.get("symbol") == "SPY"), None)
    if spy and "spySinceChatGPT" in spy:
        misc["spySinceChatGPT"] = spy["spySinceChatGPT"]

    for sym in ("SPY", "QQQ", "DIA", "IWM", "TMUS"):
        entry = next((s for s in all_stats if s.get("symbol") == sym), None)
        if entry and "return5Y" in entry: misc[f"{sym.lower()}5Y"] = entry["return5Y"]

    return misc


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
