import json
import os
from io import BytesIO
from unittest.mock import patch, MagicMock
import pytest

from src.orchestrator.app import lambda_handler, _detect_vix_spikes, _fetch_vix_spikes, BATCH_SIZE


@pytest.fixture
def mock_orchestrator_deps():
    with patch.dict("os.environ", {"BUCKET_NAME": "test-bucket", "QUEUE_URL": "https://sqs.test/queue"}), \
         patch("src.orchestrator.app.s3") as mock_s3, \
         patch("src.orchestrator.app.sqs") as mock_sqs, \
         patch("src.orchestrator.app._fetch_vix_spikes") as mock_vix:
        
        mock_sqs.get_queue_attributes.return_value = {
            "Attributes": {
                "ApproximateNumberOfMessages": "0",
                "ApproximateNumberOfMessagesNotVisible": "0",
                "ApproximateNumberOfMessagesDelayed": "0",
            }
        }
        mock_vix.return_value = []
        yield mock_s3, mock_sqs, mock_vix


def _make_s3_response(symbols: list[str]) -> dict:
    body = "\n".join(symbols).encode("utf-8")
    return {"Body": BytesIO(body)}


class TestLambdaHandler:

    def test_reads_symbols_from_s3(self, mock_orchestrator_deps):
        mock_s3, _, _ = mock_orchestrator_deps
        mock_s3.get_object.return_value = _make_s3_response(["AAPL", "MSFT"])

        lambda_handler({}, None)

        mock_s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="symbols/us-equities.txt"
        )

    def test_sends_correct_batch_count(self, mock_orchestrator_deps):
        _, mock_sqs, _ = mock_orchestrator_deps
        symbols = [f"SYM{i}" for i in range(120)] # 120 / 25 = 5 batches
        mock_orchestrator_deps[0].get_object.return_value = _make_s3_response(symbols)

        lambda_handler({}, None)

        # 5 batches sent in 1 SQS batch call (max 10 per call)
        assert mock_sqs.send_message_batch.call_count == 1
        assert len(mock_sqs.send_message_batch.call_args[1]["Entries"]) == 5

    def test_batch_message_structure(self, mock_orchestrator_deps):
        mock_s3, mock_sqs, _ = mock_orchestrator_deps
        symbols = [f"SYM{i}" for i in range(60)] # 3 batches
        mock_s3.get_object.return_value = _make_s3_response(symbols)

        lambda_handler({}, None)

        entries = mock_sqs.send_message_batch.call_args[1]["Entries"]
        assert len(entries) == 3
        
        body = json.loads(entries[0]["MessageBody"])
        assert body["batchIndex"] == 0
        assert body["totalBatches"] == 3
        assert len(body["symbols"]) == 25
        assert "runId" in body

        body2 = json.loads(entries[1]["MessageBody"])
        assert body2["batchIndex"] == 1
        assert body2["totalBatches"] == 3
        assert len(body2["symbols"]) == 25

    def test_sends_to_correct_queue_url(self, mock_orchestrator_deps):
        mock_s3, mock_sqs, _ = mock_orchestrator_deps
        mock_s3.get_object.return_value = _make_s3_response(["AAPL"])

        lambda_handler({}, None)

        assert mock_sqs.send_message_batch.call_args[1]["QueueUrl"] == "https://sqs.test/queue"

    def test_returns_summary(self, mock_orchestrator_deps):
        mock_s3, _, _ = mock_orchestrator_deps
        mock_s3.get_object.return_value = _make_s3_response(["AAPL", "MSFT", "GOOG"])

        result = lambda_handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["totalSymbols"] == 3
        assert body["totalBatches"] == 1
        assert "runId" in body

    def test_strips_whitespace_and_skips_empty_lines(self, mock_orchestrator_deps):
        mock_s3, mock_sqs, _ = mock_orchestrator_deps
        body = b"  AAPL  \n\n  MSFT \n\n\n  GOOG  \n"
        mock_s3.get_object.return_value = {"Body": BytesIO(body)}

        result = lambda_handler({}, None)

        assert json.loads(result["body"])["totalSymbols"] == 3
        entries = mock_sqs.send_message_batch.call_args[1]["Entries"]
        body_sent = json.loads(entries[0]["MessageBody"])
        assert body_sent["symbols"] == ["AAPL", "MSFT", "GOOG"]

    def test_run_id_is_date_format(self, mock_orchestrator_deps):
        mock_s3, _, _ = mock_orchestrator_deps
        mock_s3.get_object.return_value = _make_s3_response(["AAPL"])

        result = lambda_handler({}, None)

        import re
        assert re.match(r"\d{4}-\d{2}-\d{2}", json.loads(result["body"])["runId"])

    def test_exactly_batch_size_symbols(self, mock_orchestrator_deps):
        mock_s3, mock_sqs, _ = mock_orchestrator_deps
        symbols = [f"SYM{i}" for i in range(25)]
        mock_s3.get_object.return_value = _make_s3_response(symbols)

        result = lambda_handler({}, None)

        assert json.loads(result["body"])["totalBatches"] == 1
        assert mock_sqs.send_message_batch.call_count == 1

    def test_single_symbol(self, mock_orchestrator_deps):
        mock_s3, mock_sqs, _ = mock_orchestrator_deps
        mock_s3.get_object.return_value = _make_s3_response(["AAPL"])

        result = lambda_handler({}, None)

        assert json.loads(result["body"])["totalBatches"] == 1
        entries = mock_sqs.send_message_batch.call_args[1]["Entries"]
        body_sent = json.loads(entries[0]["MessageBody"])
        assert body_sent["symbols"] == ["AAPL"]
        assert body_sent["totalBatches"] == 1

    def test_message_has_no_sneak_peek(self, mock_orchestrator_deps):
        mock_s3, mock_sqs, _ = mock_orchestrator_deps
        mock_s3.get_object.return_value = _make_s3_response(["AAPL"])

        lambda_handler({}, None)

        entries = mock_sqs.send_message_batch.call_args[1]["Entries"]
        body = json.loads(entries[0]["MessageBody"])
        assert "sneakPeek" not in body

    def test_includes_vix_spikes_in_message(self, mock_orchestrator_deps):
        mock_s3, mock_sqs, mock_vix = mock_orchestrator_deps
        spikes = [{"dateString": "3/10/25", "timestamp": 1000, "vixClose": 25.0}]
        mock_vix.return_value = spikes
        mock_s3.get_object.return_value = _make_s3_response(["AAPL"])

        lambda_handler({}, None)

        entries = mock_sqs.send_message_batch.call_args[1]["Entries"]
        body = json.loads(entries[0]["MessageBody"])
        assert body["vixSpikes"] == spikes

    def test_empty_vix_spikes_in_message(self, mock_orchestrator_deps):
        mock_s3, mock_sqs, mock_vix = mock_orchestrator_deps
        mock_vix.return_value = []
        mock_s3.get_object.return_value = _make_s3_response(["AAPL"])

        lambda_handler({}, None)

        entries = mock_sqs.send_message_batch.call_args[1]["Entries"]
        body = json.loads(entries[0]["MessageBody"])
        assert body["vixSpikes"] == []

    def test_return_includes_vix_spike_count(self, mock_orchestrator_deps):
        mock_s3, _, mock_vix = mock_orchestrator_deps
        spikes = [
            {"dateString": "3/10/25", "timestamp": 1000, "vixClose": 25.0},
            {"dateString": "8/5/25", "timestamp": 2000, "vixClose": 30.0},
        ]
        mock_vix.return_value = spikes
        mock_s3.get_object.return_value = _make_s3_response(["AAPL"])

        result = lambda_handler({}, None)

        assert json.loads(result["body"])["vixSpikes"] == 2


class TestBatchSize:

    def test_batch_size_is_25(self):
        assert BATCH_SIZE == 25


class TestDetectVixSpikes:

    def test_no_spikes_below_threshold(self):
        closes = [15.0, 18.0, 19.0]
        timestamps = [1000, 2000, 3000]
        assert _detect_vix_spikes(closes, timestamps) == []

    def test_single_spike(self):
        closes = [15.0, 25.0, 15.0]
        timestamps = [1000, 86400, 172800]
        result = _detect_vix_spikes(closes, timestamps)
        assert len(result) == 1
        assert result[0]["vixClose"] == 25.0

    def test_clusters_nearby_spikes(self):
        day = 86400
        closes = [25.0, 30.0, 15.0, 15.0, 15.0, 15.0, 15.0, 15.0, 15.0, 15.0, 15.0, 15.0, 28.0]
        timestamps = [day * i for i in range(13)]
        result = _detect_vix_spikes(closes, timestamps)
        assert len(result) == 2
        assert result[0]["vixClose"] == 30.0
        assert result[1]["vixClose"] == 28.0

    def test_empty_input(self):
        assert _detect_vix_spikes([], []) == []

    def test_mismatched_lengths(self):
        assert _detect_vix_spikes([25.0], [1000, 2000]) == []


class TestFetchVixSpikes:

    @patch("src.orchestrator.app.urllib.request.urlopen")
    def test_success_returns_spikes(self, mock_urlopen):
        response_data = {
            "chart": {
                "result": [{
                    "timestamp": [86400, 172800, 259200],
                    "indicators": {"quote": [{"close": [15.0, 25.0, 15.0]}]},
                }]
            }
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = _fetch_vix_spikes()

        assert len(result) == 1
        assert result[0]["vixClose"] == 25.0

    @patch("src.orchestrator.app.urllib.request.urlopen")
    def test_network_error_returns_empty(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("no network")

        assert _fetch_vix_spikes() == []

    @patch("src.orchestrator.app.urllib.request.urlopen")
    def test_malformed_json_returns_empty(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = b"not json"
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        assert _fetch_vix_spikes() == []

    @patch("src.orchestrator.app.urllib.request.urlopen")
    def test_missing_chart_key_returns_empty(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"other": {}}).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        assert _fetch_vix_spikes() == []

    @patch("src.orchestrator.app.urllib.request.urlopen")
    def test_all_null_closes_returns_empty(self, mock_urlopen):
        response_data = {
            "chart": {
                "result": [{
                    "timestamp": [1000, 2000],
                    "indicators": {"quote": [{"close": [None, None]}]},
                }]
            }
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        assert _fetch_vix_spikes() == []

    @patch("src.orchestrator.app.urllib.request.urlopen")
    def test_no_spikes_above_threshold_returns_empty(self, mock_urlopen):
        response_data = {
            "chart": {
                "result": [{
                    "timestamp": [1000, 2000, 3000],
                    "indicators": {"quote": [{"close": [12.0, 15.0, 18.0]}]},
                }]
            }
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        assert _fetch_vix_spikes() == []
