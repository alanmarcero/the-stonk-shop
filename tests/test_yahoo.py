from unittest.mock import patch, MagicMock
import json

from src.worker.yahoo import (
    fetch_daily_candles, fetch_monthly_candles,
    fetch_quarterly_candles, fetch_forward_pe, fetch_vix_candles,
    fetch_stats_candles,
    _parse_response, _parse_forward_pe, _parse_forward_pe_history,
    BASE_URL, TIMESERIES_URL, USER_AGENT, TIMEOUT_SECONDS,
)


class TestParseResponse:

    def test_valid_response(self):
        data = {
            "chart": {
                "result": [{
                    "meta": {"shortName": "Apple Inc."},
                    "timestamp": [1000, 2000, 3000],
                    "indicators": {
                        "quote": [{"close": [100.0, 101.5, 102.0]}]
                    },
                }]
            }
        }

        result = _parse_response(data)

        assert result is not None
        closes, timestamps, name = result
        assert closes == [100.0, 101.5, 102.0]
        assert timestamps == [1000, 2000, 3000]
        assert name == "Apple Inc."

    def test_valid_response_long_name_fallback(self):
        data = {
            "chart": {
                "result": [{
                    "meta": {"longName": "Microsoft Corporation"},
                    "timestamp": [1000],
                    "indicators": {"quote": [{"close": [200.0]}]},
                }]
            }
        }
        closes, timestamps, name = _parse_response(data)
        assert name == "Microsoft Corporation"

    def test_filters_null_closes(self):
        data = {
            "chart": {
                "result": [{
                    "meta": {"shortName": "Test"},
                    "timestamp": [1000, 2000, 3000, 4000],
                    "indicators": {
                        "quote": [{"close": [100.0, None, 102.0, None]}]
                    },
                }]
            }
        }

        result = _parse_response(data)

        closes, timestamps, name = result
        assert closes == [100.0, 102.0]
        assert timestamps == [1000, 3000]

    def test_all_null_closes_returns_none(self):
        data = {
            "chart": {
                "result": [{
                    "timestamp": [1000, 2000],
                    "indicators": {
                        "quote": [{"close": [None, None]}]
                    },
                }]
            }
        }

        assert _parse_response(data) is None

    def test_missing_chart_key(self):
        assert _parse_response({}) is None

    def test_missing_result(self):
        assert _parse_response({"chart": {}}) is None

    def test_empty_result_list(self):
        assert _parse_response({"chart": {"result": []}}) is None

    def test_missing_timestamp(self):
        data = {
            "chart": {
                "result": [{
                    "indicators": {"quote": [{"close": [100.0]}]},
                }]
            }
        }
        assert _parse_response(data) is None

    def test_missing_indicators(self):
        data = {
            "chart": {
                "result": [{
                    "timestamp": [1000],
                }]
            }
        }
        assert _parse_response(data) is None

    def test_missing_quote(self):
        data = {
            "chart": {
                "result": [{
                    "timestamp": [1000],
                    "indicators": {},
                }]
            }
        }
        assert _parse_response(data) is None

    def test_missing_close_key(self):
        data = {
            "chart": {
                "result": [{
                    "timestamp": [1000],
                    "indicators": {"quote": [{"open": [100.0]}]},
                }]
            }
        }
        assert _parse_response(data) is None

    def test_null_result(self):
        assert _parse_response({"chart": {"result": None}}) is None

    def test_single_valid_close(self):
        data = {
            "chart": {
                "result": [{
                    "meta": {"shortName": "Test"},
                    "timestamp": [1000],
                    "indicators": {"quote": [{"close": [99.5]}]},
                }]
            }
        }

        closes, timestamps, name = _parse_response(data)
        assert closes == [99.5]
        assert timestamps == [1000]
        assert name == "Test"


class TestFetchDailyCandles:

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        response_data = {
            "chart": {
                "result": [{
                    "meta": {"shortName": "AAPL"},
                    "timestamp": [1000, 2000],
                    "indicators": {"quote": [{"close": [150.0, 155.0]}]},
                }]
            }
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = fetch_daily_candles("AAPL")

        assert result is not None
        closes, timestamps, name = result
        assert closes == [150.0, 155.0]
        assert timestamps == [1000, 2000]
        assert name == "AAPL"

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_builds_correct_url(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("stop")

        fetch_daily_candles("MSFT")

        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        assert request.full_url == f"{BASE_URL}/MSFT?range=1mo&interval=1d"

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_sets_user_agent(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("stop")

        fetch_daily_candles("GOOG")

        request = mock_urlopen.call_args[0][0]
        assert request.get_header("User-agent") == USER_AGENT

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_sets_timeout(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("stop")

        fetch_daily_candles("TSLA")

        assert mock_urlopen.call_args[1]["timeout"] == TIMEOUT_SECONDS

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_network_error_returns_none(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("no network")

        assert fetch_daily_candles("FAIL") is None

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_invalid_json_returns_none(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = b"not json"
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        assert fetch_daily_candles("BAD") is None


class TestFetchQuarterlyCandles:

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_builds_correct_url(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("stop")

        fetch_quarterly_candles("MSFT")

        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        assert request.full_url == f"{BASE_URL}/MSFT?range=5y&interval=1wk"

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        response_data = {
            "chart": {
                "result": [{
                    "meta": {"shortName": "AAPL"},
                    "timestamp": [1771218000, 1771822800],
                    "indicators": {"quote": [{"close": [150.0, 155.0]}]},
                }]
            }
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = fetch_quarterly_candles("AAPL")

        assert result is not None
        closes, timestamps, name = result
        assert closes == [150.0, 155.0]
        assert timestamps == [1771218000, 1771822800]
        assert name == "AAPL"

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_network_error_returns_none(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("no network")

        assert fetch_quarterly_candles("FAIL") is None


class TestFetchMonthlyCandles:

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        response_data = {
            "chart": {
                "result": [{
                    "meta": {"shortName": "AAPL"},
                    "timestamp": [1771218000, 1771822800],
                    "indicators": {"quote": [{"close": [150.0, 155.0]}]},
                }]
            }
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = fetch_monthly_candles("AAPL")

        assert result is not None
        closes, timestamps, name = result
        assert closes == [150.0, 155.0]
        assert timestamps == [1771218000, 1771822800]
        assert name == "AAPL"

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_builds_correct_url(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("stop")

        fetch_monthly_candles("MSFT")

        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        assert request.full_url == f"{BASE_URL}/MSFT?range=2y&interval=1wk"

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_network_error_returns_none(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("no network")

        assert fetch_monthly_candles("FAIL") is None


class TestFetchVixCandles:

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_builds_correct_url(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("stop")

        fetch_vix_candles()

        request = mock_urlopen.call_args[0][0]
        assert "^VIX" in request.full_url
        assert "range=3y" in request.full_url
        assert "interval=1d" in request.full_url


class TestFetchForwardPE:

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        response_data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"asOfDate": "2025-06-30", "reportedValue": {"raw": 18.5}},
                        {"asOfDate": "2025-09-30", "reportedValue": {"raw": 20.3}},
                    ]
                }]
            }
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        current, history = fetch_forward_pe("AAPL")

        assert current == 20.3
        assert history == {"Q2'25": 18.5, "Q3'25": 20.3}

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_builds_correct_url(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("stop")

        fetch_forward_pe("MSFT")

        request = mock_urlopen.call_args[0][0]
        assert TIMESERIES_URL + "/MSFT" in request.full_url
        assert "quarterlyForwardPeRatio" in request.full_url

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_network_error_returns_none(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("no network")

        current, history = fetch_forward_pe("FAIL")
        assert current is None
        assert history is None


class TestParseForwardPE:

    def test_valid_response(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"reportedValue": {"raw": 15.7}},
                    ]
                }]
            }
        }
        assert _parse_forward_pe(data) == 15.7

    def test_multiple_entries_takes_last(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"reportedValue": {"raw": 10.0}},
                        {"reportedValue": {"raw": 25.0}},
                    ]
                }]
            }
        }
        assert _parse_forward_pe(data) == 25.0

    def test_empty_entries_returns_none(self):
        data = {"timeseries": {"result": [{"quarterlyForwardPeRatio": []}]}}
        assert _parse_forward_pe(data) is None

    def test_missing_key_returns_none(self):
        assert _parse_forward_pe({}) is None
        assert _parse_forward_pe({"timeseries": {}}) is None
        assert _parse_forward_pe({"timeseries": {"result": []}}) is None

    def test_no_quarterly_key_returns_none(self):
        data = {"timeseries": {"result": [{"otherKey": []}]}}
        assert _parse_forward_pe(data) is None

    def test_rounds_to_2_decimals(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"reportedValue": {"raw": 18.12345}},
                    ]
                }]
            }
        }
        assert _parse_forward_pe(data) == 18.12


class TestParseForwardPEHistory:

    def test_valid_response(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"asOfDate": "2025-03-31", "reportedValue": {"raw": 15.7}},
                        {"asOfDate": "2025-06-30", "reportedValue": {"raw": 18.2}},
                    ]
                }]
            }
        }
        result = _parse_forward_pe_history(data)
        assert result == {"Q1'25": 15.7, "Q2'25": 18.2}

    def test_single_entry(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"asOfDate": "2025-12-31", "reportedValue": {"raw": 22.0}},
                    ]
                }]
            }
        }
        assert _parse_forward_pe_history(data) == {"Q4'25": 22.0}

    def test_missing_as_of_date_skipped(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"reportedValue": {"raw": 15.0}},
                        {"asOfDate": "2025-09-30", "reportedValue": {"raw": 20.0}},
                    ]
                }]
            }
        }
        assert _parse_forward_pe_history(data) == {"Q3'25": 20.0}

    def test_empty_entries_returns_none(self):
        data = {"timeseries": {"result": [{"quarterlyForwardPeRatio": []}]}}
        assert _parse_forward_pe_history(data) is None

    def test_missing_key_returns_none(self):
        assert _parse_forward_pe_history({}) is None

    def test_rounds_to_2_decimals(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"asOfDate": "2025-03-31", "reportedValue": {"raw": 18.12345}},
                    ]
                }]
            }
        }
        assert _parse_forward_pe_history(data) == {"Q1'25": 18.12}

    def test_all_four_quarters(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"asOfDate": "2025-03-31", "reportedValue": {"raw": 15.0}},
                        {"asOfDate": "2025-06-30", "reportedValue": {"raw": 16.0}},
                        {"asOfDate": "2025-09-30", "reportedValue": {"raw": 17.0}},
                        {"asOfDate": "2025-12-31", "reportedValue": {"raw": 18.0}},
                    ]
                }]
            }
        }
        result = _parse_forward_pe_history(data)
        assert result == {"Q1'25": 15.0, "Q2'25": 16.0, "Q3'25": 17.0, "Q4'25": 18.0}

    def test_malformed_date_skipped(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"asOfDate": "baddate", "reportedValue": {"raw": 15.0}},
                        {"asOfDate": "2025-06-30", "reportedValue": {"raw": 18.0}},
                    ]
                }]
            }
        }
        assert _parse_forward_pe_history(data) == {"Q2'25": 18.0}

    def test_missing_raw_value_skipped(self):
        data = {
            "timeseries": {
                "result": [{
                    "quarterlyForwardPeRatio": [
                        {"asOfDate": "2025-03-31", "reportedValue": {}},
                        {"asOfDate": "2025-06-30", "reportedValue": {"raw": 20.0}},
                    ]
                }]
            }
        }
        assert _parse_forward_pe_history(data) == {"Q2'25": 20.0}


class TestFetchStatsCandles:

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_builds_correct_url(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("stop")

        fetch_stats_candles("AAPL")

        request = mock_urlopen.call_args[0][0]
        assert request.full_url == f"{BASE_URL}/AAPL?range=5y&interval=1d"

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        response_data = {
            "chart": {
                "result": [{
                    "meta": {"shortName": "AAPL"},
                    "timestamp": [1000, 2000],
                    "indicators": {"quote": [{"close": [150.0, 155.0]}]},
                }]
            }
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = fetch_stats_candles("AAPL")

        assert result is not None
        closes, timestamps, name = result
        assert closes == [150.0, 155.0]
        assert name == "AAPL"

    @patch("src.worker.yahoo.urllib.request.urlopen")
    def test_network_error_returns_none(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("no network")

        assert fetch_stats_candles("FAIL") is None
