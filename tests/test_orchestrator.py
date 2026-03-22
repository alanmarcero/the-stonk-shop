import json
from io import BytesIO
from unittest.mock import patch, MagicMock

from src.orchestrator.app import lambda_handler, _detect_vix_spikes, _fetch_vix_spikes


class TestLambdaHandler:

    def setup_method(self):
        self._env_patcher = patch.dict(
            "os.environ", {"BUCKET_NAME": "test-bucket", "QUEUE_URL": "https://sqs.test/queue"}
        )
        self._s3_patcher = patch("src.orchestrator.app.s3")
        self._sqs_patcher = patch("src.orchestrator.app.sqs")
        self._vix_patcher = patch("src.orchestrator.app._fetch_vix_spikes")
        self._env_patcher.start()
        self.mock_s3 = self._s3_patcher.start()
        self.mock_sqs = self._sqs_patcher.start()
        self.mock_vix = self._vix_patcher.start()
        self.mock_vix.return_value = []

    def teardown_method(self):
        self._vix_patcher.stop()
        self._sqs_patcher.stop()
        self._s3_patcher.stop()
        self._env_patcher.stop()

    def _make_s3_response(self, symbols: list[str]) -> dict:
        body = "\n".join(symbols).encode("utf-8")
        return {"Body": BytesIO(body)}

    def test_reads_symbols_from_s3(self):
        self.mock_s3.get_object.return_value = self._make_s3_response(["AAPL", "MSFT"])

        lambda_handler({}, None)

        self.mock_s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="symbols/us-equities.txt"
        )

    def test_sends_correct_batch_count(self):
        symbols = [f"SYM{i}" for i in range(120)]
        self.mock_s3.get_object.return_value = self._make_s3_response(symbols)

        lambda_handler({}, None)

        assert self.mock_sqs.send_message.call_count == 3

    def test_batch_message_structure(self):
        symbols = [f"SYM{i}" for i in range(60)]
        self.mock_s3.get_object.return_value = self._make_s3_response(symbols)

        lambda_handler({}, None)

        first_call = self.mock_sqs.send_message.call_args_list[0]
        body = json.loads(first_call[1]["MessageBody"])
        assert body["batchIndex"] == 0
        assert body["totalBatches"] == 2
        assert len(body["symbols"]) == 50
        assert "runId" in body

        second_call = self.mock_sqs.send_message.call_args_list[1]
        body2 = json.loads(second_call[1]["MessageBody"])
        assert body2["batchIndex"] == 1
        assert body2["totalBatches"] == 2
        assert len(body2["symbols"]) == 10

    def test_sends_to_correct_queue_url(self):
        self.mock_s3.get_object.return_value = self._make_s3_response(["AAPL"])

        lambda_handler({}, None)

        assert self.mock_sqs.send_message.call_args[1]["QueueUrl"] == "https://sqs.test/queue"

    def test_returns_summary(self):
        self.mock_s3.get_object.return_value = self._make_s3_response(["AAPL", "MSFT", "GOOG"])

        result = lambda_handler({}, None)

        assert result["statusCode"] == 200
        assert result["body"]["totalSymbols"] == 3
        assert result["body"]["totalBatches"] == 1
        assert "runId" in result["body"]

    def test_strips_whitespace_and_skips_empty_lines(self):
        body = b"  AAPL  \n\n  MSFT \n\n\n  GOOG  \n"
        self.mock_s3.get_object.return_value = {"Body": BytesIO(body)}

        result = lambda_handler({}, None)

        assert result["body"]["totalSymbols"] == 3
        body_sent = json.loads(self.mock_sqs.send_message.call_args[1]["MessageBody"])
        assert body_sent["symbols"] == ["AAPL", "MSFT", "GOOG"]

    def test_run_id_is_date_format(self):
        self.mock_s3.get_object.return_value = self._make_s3_response(["AAPL"])

        result = lambda_handler({}, None)

        import re
        assert re.match(r"\d{4}-\d{2}-\d{2}", result["body"]["runId"])

    def test_exactly_batch_size_symbols(self):
        symbols = [f"SYM{i}" for i in range(50)]
        self.mock_s3.get_object.return_value = self._make_s3_response(symbols)

        result = lambda_handler({}, None)

        assert result["body"]["totalBatches"] == 1
        assert self.mock_sqs.send_message.call_count == 1

    def test_single_symbol(self):
        self.mock_s3.get_object.return_value = self._make_s3_response(["AAPL"])

        result = lambda_handler({}, None)

        assert result["body"]["totalBatches"] == 1
        body_sent = json.loads(self.mock_sqs.send_message.call_args[1]["MessageBody"])
        assert body_sent["symbols"] == ["AAPL"]
        assert body_sent["totalBatches"] == 1

    def test_message_has_no_sneak_peek(self):
        self.mock_s3.get_object.return_value = self._make_s3_response(["AAPL"])

        lambda_handler({}, None)

        body = json.loads(self.mock_sqs.send_message.call_args[1]["MessageBody"])
        assert "sneakPeek" not in body

    def test_includes_vix_spikes_in_message(self):
        spikes = [{"dateString": "3/10/25", "timestamp": 1000, "vixClose": 25.0}]
        self.mock_vix.return_value = spikes
        self.mock_s3.get_object.return_value = self._make_s3_response(["AAPL"])

        lambda_handler({}, None)

        body = json.loads(self.mock_sqs.send_message.call_args[1]["MessageBody"])
        assert body["vixSpikes"] == spikes

    def test_empty_vix_spikes_in_message(self):
        self.mock_vix.return_value = []
        self.mock_s3.get_object.return_value = self._make_s3_response(["AAPL"])

        lambda_handler({}, None)

        body = json.loads(self.mock_sqs.send_message.call_args[1]["MessageBody"])
        assert body["vixSpikes"] == []

    def test_return_includes_vix_spike_count(self):
        spikes = [
            {"dateString": "3/10/25", "timestamp": 1000, "vixClose": 25.0},
            {"dateString": "8/5/25", "timestamp": 2000, "vixClose": 30.0},
        ]
        self.mock_vix.return_value = spikes
        self.mock_s3.get_object.return_value = self._make_s3_response(["AAPL"])

        result = lambda_handler({}, None)

        assert result["body"]["vixSpikes"] == 2


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
        # Two spike clusters separated by 10 non-spike days (well beyond gap_days=5)
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
