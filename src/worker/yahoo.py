import json
import time
import urllib.request
from typing import Optional

BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
TIMESERIES_URL = "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries"
USER_AGENT = "Mozilla/5.0"
TIMEOUT_SECONDS = 10


def fetch_daily_candles(symbol: str) -> Optional[tuple[list[float], list[int], str]]:
    return _fetch_candles(symbol, range_param="1mo", interval="1d")


def fetch_monthly_candles(symbol: str) -> Optional[tuple[list[float], list[int], str]]:
    return _fetch_candles(symbol, range_param="2y", interval="1wk")


def fetch_quarterly_candles(symbol: str) -> Optional[tuple[list[float], list[int], str]]:
    return _fetch_candles(symbol, range_param="5y", interval="1wk")


def fetch_stats_candles(symbol: str) -> Optional[tuple[list[float], list[int], str]]:
    return _fetch_candles(symbol, range_param="5y", interval="1d")


def fetch_vix_candles() -> Optional[tuple[list[float], list[int], str]]:
    return _fetch_candles("^VIX", range_param="3y", interval="1d")


def _fetch_candles(symbol: str, range_param: str, interval: str) -> Optional[tuple[list[float], list[int], str]]:
    url = f"{BASE_URL}/{symbol}?range={range_param}&interval={interval}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            response_data = json.loads(response.read())
    except (OSError, ValueError) as err:
        print(f"[yahoo] {symbol}: {err}")
        return None

    return _parse_response(response_data)


def fetch_forward_pe(symbol: str) -> tuple[Optional[float], Optional[dict]]:
    """Fetch forward P/E ratio from Yahoo Timeseries API.

    Returns (current_pe, pe_history) where pe_history maps quarter labels
    like "Q3'25" to P/E values.
    """
    now = int(time.time())
    two_years_ago = now - 2 * 365 * 86400
    url = (
        f"{TIMESERIES_URL}/{symbol}"
        f"?type=quarterlyForwardPeRatio"
        f"&period1={two_years_ago}&period2={now}"
    )
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            response_data = json.loads(response.read())
    except (OSError, ValueError) as err:
        print(f"[yahoo] {symbol} forward PE: {err}")
        return None, None

    current = _parse_forward_pe(response_data)
    history = _parse_forward_pe_history(response_data)
    return current, history


def _parse_forward_pe(response_data: dict) -> Optional[float]:
    """Extract the most recent forward P/E value from timeseries response."""
    try:
        results = response_data.get("timeseries", {}).get("result", [])
        for entry_group in results:
            if entries := entry_group.get("quarterlyForwardPeRatio"):
                return round(entries[-1]["reportedValue"]["raw"], 2)
    except (KeyError, IndexError, TypeError, ValueError) as err:
        print(f"[yahoo] parse error: {err}")
    return None


def _parse_forward_pe_history(response_data: dict) -> Optional[dict]:
    """Parse all quarterly forward P/E entries into a dict of quarter labels."""
    try:
        results = response_data.get("timeseries", {}).get("result", [])
        for entry_group in results:
            if not (entries := entry_group.get("quarterlyForwardPeRatio")):
                continue

            history = {
                f"Q{(int(parts[1]) - 1) // 3 + 1}'{parts[0][-2:]}": round(entry["reportedValue"]["raw"], 2)
                for entry in entries
                if (date_str := entry.get("asOfDate", ""))
                and (raw := entry.get("reportedValue", {}).get("raw")) is not None
                and len(parts := date_str.split("-")) == 3
            }
            return history if history else None
    except (KeyError, IndexError, TypeError, ValueError) as err:
        print(f"[yahoo] parse error: {err}")
    return None



def _parse_response(response_data: dict) -> Optional[tuple[list[float], list[int], str]]:
    """Parse Yahoo chart API response into (closes, timestamps, short_name) or None."""
    try:
        chart_result = response_data["chart"]["result"][0]
        meta = chart_result.get("meta", {})
        short_name = meta.get("shortName") or meta.get("longName") or ""
        raw_timestamps = chart_result["timestamp"]
        raw_closes = chart_result["indicators"]["quote"][0]["close"]
    except (KeyError, IndexError, TypeError):
        return None

    valid_closes = [
        (close, ts)
        for close, ts in zip(raw_closes, raw_timestamps)
        if close is not None
    ]

    if not valid_closes:
        return None

    closes = [close for close, _ in valid_closes]
    timestamps = [ts for _, ts in valid_closes]

    return closes, timestamps, short_name
