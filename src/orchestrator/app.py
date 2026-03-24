import json
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any

import boto3

BATCH_SIZE = 25
VIX_URL = "https://query1.finance.yahoo.com/v8/finance/chart/^VIX?range=3y&interval=1d"
USER_AGENT = "Mozilla/5.0"
VIX_TIMEOUT = 15
VIX_THRESHOLD = 20.0
VIX_GAP_DAYS = 5

s3 = boto3.client("s3")
sqs = boto3.client("sqs")


def lambda_handler(event: dict, context: Any) -> dict:
    bucket = os.environ["BUCKET_NAME"]
    queue_url = os.environ["QUEUE_URL"]

    is_http = "requestContext" in event and "http" in event["requestContext"]
    if is_http:
        qs = event.get("queryStringParameters", {})
        if qs.get("dev_key") != "stonks":
            return {"statusCode": 403, "body": json.dumps({"error": "Forbidden"})}

    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )
    attr_dict = attrs.get("Attributes", {})
    in_flight = sum(int(attr_dict.get(k, 0)) for k in [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ])
    
    if is_http and event["requestContext"]["http"]["method"] == "GET":
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"running": in_flight > 0, "inFlight": in_flight})
        }

    if in_flight > 0:
        msg = f"Scan already in progress (queue size: {in_flight})"
        print(f"[orchestrator] {msg}")
        return {"statusCode": 429, "body": json.dumps({"error": msg})}

    resp = s3.get_object(Bucket=bucket, Key="symbols/us-equities.txt")
    lines = resp["Body"].read().decode("utf-8").splitlines()
    symbols = [line.strip() for line in lines if line.strip()]

    vix_spikes = _fetch_vix_spikes()

    snapshot = event.get("snapshot", False)

    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    batches = [symbols[i : i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    total_batches = len(batches)

    # Send messages in batches of 10 for efficiency (SQS limit)
    sqs_messages = []
    for idx, batch in enumerate(batches):
        sqs_messages.append({
            "Id": f"batch_{idx}",
            "MessageBody": json.dumps({
                "runId": run_id,
                "batchIndex": idx,
                "totalBatches": total_batches,
                "symbols": batch,
                "vixSpikes": vix_spikes,
                "snapshot": snapshot,
            })
        })

    for i in range(0, len(sqs_messages), 10):
        sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=sqs_messages[i : i + 10]
        )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "runId": run_id,
            "totalSymbols": len(symbols),
            "totalBatches": total_batches,
            "vixSpikes": len(vix_spikes),
        }),
    }


def _fetch_vix_spikes() -> list[dict]:
    """Fetch ^VIX 3yr daily data and detect spike clusters."""
    request = urllib.request.Request(VIX_URL, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=VIX_TIMEOUT) as response:
            response_data = json.loads(response.read())
    except (OSError, ValueError) as err:
        print(f"[orchestrator] VIX fetch failed: {err}")
        return []

    try:
        chart_result = response_data["chart"]["result"][0]
        raw_timestamps = chart_result["timestamp"]
        raw_closes = chart_result["indicators"]["quote"][0]["close"]
    except (KeyError, IndexError, TypeError):
        print("[orchestrator] VIX parse failed")
        return []

    valid_closes = [(c, t) for c, t in zip(raw_closes, raw_timestamps) if c is not None]
    if not valid_closes:
        return []

    closes = [c for c, _ in valid_closes]
    timestamps = [t for _, t in valid_closes]

    return _detect_vix_spikes(closes, timestamps)


def _detect_vix_spikes(
    closes: list[float],
    timestamps: list[int],
) -> list[dict]:
    """Detect VIX spike clusters. Returns list of {dateString, timestamp, vixClose}."""
    if len(closes) != len(timestamps) or not closes:
        return []

    spike_indices = [i for i, c in enumerate(closes) if c >= VIX_THRESHOLD]
    if not spike_indices:
        return []

    clusters: list[list[int]] = []
    current_cluster: list[int] = [spike_indices[0]]

    for i in range(1, len(spike_indices)):
        gap = spike_indices[i] - spike_indices[i - 1]
        if gap <= VIX_GAP_DAYS + 1:
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
