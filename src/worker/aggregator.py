from typing import Dict, List, Tuple

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


def aggregate_batches(batches: List[dict]) -> Tuple[Dict[str, List[dict]], int, int]:
    aggregated: Dict[str, List[dict]] = {key: [] for key, _ in _AGGREGATE_KEYS}
    aggregated.update({"stats": [], "errorDetails": []})
    total_symbols = 0
    total_errors = 0

    for batch in batches:
        if batch is None:
            continue
        for json_key, _ in _AGGREGATE_KEYS:
            aggregated[json_key].extend(batch.get(json_key, []))
        aggregated["stats"].extend(batch.get("stats", []))
        aggregated["errorDetails"].extend(batch.get("errorDetails", []))
        total_symbols += batch.get("symbolsProcessed", 0)
        total_errors += batch.get("errors", 0)

    _sort_aggregated(aggregated)
    return aggregated, total_symbols, total_errors


def _sort_aggregated(aggregated: Dict[str, List[dict]]) -> None:
    for json_key, sort_field in _AGGREGATE_KEYS:
        aggregated[json_key].sort(key=lambda x, f=sort_field: x.get(f, 0), reverse=True)
