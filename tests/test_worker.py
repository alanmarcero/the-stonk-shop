import json
import os
from unittest.mock import patch, MagicMock
import pytest

from src.worker.app import (
    lambda_handler,
    _process_batch,
    _aggregate_to_monthly,
    _aggregate_to_quarterly,
    _strip_incomplete_week,
    _update_manifest,
    _delete_snapshot,
    _compute_misc_stats,
    BatchResult,
    MAX_WEEKLY_SNAPSHOTS,
)


# -- Shared test data --

CROSSOVER_CLOSES = [100.0, 102.0, 104.0, 106.0, 108.0, 100.0, 101.0, 101.0, 106.0]
BELOW_CLOSES = [100.0, 102.0, 104.0, 106.0, 108.0, 100.0, 101.0, 101.0]
UPTREND_CLOSES = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
CROSSDOWN_CLOSES = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 40.0]
EMPTY_BATCH = {"symbolsProcessed": 10, "errors": 0, "errorDetails": [], "crossovers": [], "crossdowns": [], "weekBelow": [], "weekAbove": [], "monthCrossovers": [], "monthCrossdowns": [], "monthBelow": [], "monthAbove": [], "quarterCrossovers": [], "quarterCrossdowns": [], "quarterBelow": [], "quarterAbove": [], "stats": []}


def _timestamps_for(closes):
    return list(range(len(closes)))


@pytest.fixture(autouse=True)
def mock_worker_deps():
    with patch("src.worker.app.yahoo") as mock_yahoo, \
         patch("src.worker.app.time") as mock_time, \
         patch("src.worker.app.stats") as mock_stats:
        mock_yahoo.fetch_quarterly_candles.return_value = None
        mock_yahoo.fetch_stats_candles.return_value = None
        mock_yahoo.fetch_forward_pe.return_value = (None, None)
        mock_stats.compute_stats.return_value = None
        yield mock_yahoo, mock_time, mock_stats


@pytest.fixture
def mock_storage():
    with patch("src.worker.storage.s3") as mock_s3, \
         patch("src.worker.storage.cloudfront") as mock_cf:
        yield mock_s3, mock_cf


@pytest.fixture
def mock_agg_deps():
    with patch("src.worker.storage.read_json") as mock_read, \
         patch("src.worker.storage.put_json") as mock_put, \
         patch("src.worker.app._update_manifest") as mock_manifest:
        yield mock_read, mock_put, mock_manifest


class TestProcessBatch:

    def test_crossover_detected(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES), "Test Name")
        result = _process_batch(["TEST"])
        assert len(result.crossovers) == 1
        assert result.crossovers[0]["symbol"] == "TEST"
        assert result.crossovers[0]["weeksBelow"] == 3

    def test_crossover_output_fields(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES), "Apple Inc.")
        result = _process_batch(["AAPL"])
        entry = result.crossovers[0]
        assert set(entry.keys()) == {"symbol", "name", "close", "ema", "pctAbove", "weeksBelow"}

    def test_crossover_ema_rounded_to_4_decimals(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES), "X")
        result = _process_batch(["X"])
        ema_str = str(result.crossovers[0]["ema"])
        decimals = ema_str.split(".")[-1] if "." in ema_str else ""
        assert len(decimals) <= 4

    def test_crossover_pct_above_rounded_to_2_decimals(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES), "X")
        result = _process_batch(["X"])
        pct = result.crossovers[0]["pctAbove"]
        assert pct == round(pct, 2)

    def test_week_below_detected_with_minimum_weeks(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES), "Test Name")
        result = _process_batch(["TEST"])
        assert len(result.week_below) == 1
        assert result.week_below[0]["count"] == 3

    def test_week_below_output_fields(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES), "X")
        result = _process_batch(["X"])
        assert set(result.week_below[0].keys()) == {"symbol", "name", "close", "ema", "pctBelow", "count"}

    def test_week_below_not_detected_under_threshold(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        closes = [50.0, 52.0, 54.0, 56.0, 58.0, 56.0, 53.0]
        mock_yahoo.fetch_quarterly_candles.return_value = (closes, _timestamps_for(closes), "Test")
        result = _process_batch(["TEST"])
        assert len(result.week_below) == 0

    def test_uptrend_no_crossover_no_below(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = (UPTREND_CLOSES, _timestamps_for(UPTREND_CLOSES), "Bull")
        result = _process_batch(["BULL"])
        assert len(result.crossovers) == 0
        assert len(result.week_below) == 0

    def test_fetch_failure_records_error(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = None
        result = _process_batch(["FAIL"])
        assert len(result.errors) == 1

    def test_multiple_symbols_rate_limited(self, mock_worker_deps):
        mock_yahoo, mock_time, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = ([50.0] * 10, list(range(10)), "A")
        _process_batch(["A", "B", "C"])
        assert mock_time.sleep.call_count == 2

    def test_single_symbol_no_sleep(self, mock_worker_deps):
        mock_yahoo, mock_time, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = ([50.0] * 10, list(range(10)), "Only")
        _process_batch(["ONLY"])
        mock_time.sleep.assert_not_called()

    def test_empty_batch(self, mock_worker_deps):
        result = _process_batch([])
        assert result.crossovers == []
        assert result.errors == []

    def test_stats_computed_when_stats_candles_available(self, mock_worker_deps):
        mock_yahoo, _, mock_stats = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = ([50.0] * 10, list(range(10)), "AAPL")
        mock_yahoo.fetch_stats_candles.return_value = ([100.0, 105.0], [1000, 2000], "AAPL")
        mock_stats.compute_stats.return_value = {"close": 105.0, "ytdPct": 5.0}
        result = _process_batch(["AAPL"])
        assert len(result.stats_data) == 1
        assert result.stats_data[0]["symbol"] == "AAPL"

    def test_crossdown_detected(self, mock_worker_deps):
        mock_yahoo, _, _ = mock_worker_deps
        mock_yahoo.fetch_quarterly_candles.return_value = (CROSSDOWN_CLOSES, _timestamps_for(CROSSDOWN_CLOSES), "Test")
        result = _process_batch(["TEST"])
        assert len(result.crossdowns) == 1
        assert result.crossdowns[0]["weeksAbove"] == 4


class TestStripIncompleteWeek:
    def test_strips_current_week_candle(self):
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        current_monday = now - timedelta(days=now.weekday())
        current_ts = int(current_monday.replace(hour=0, minute=0, second=0).timestamp())
        last_monday_ts = int((current_monday - timedelta(weeks=1)).replace(hour=0, minute=0, second=0).timestamp())
        closes = [100.0, 101.0, 102.0]
        timestamps = [last_monday_ts - 604800, last_monday_ts, current_ts]
        result_closes, result_ts = _strip_incomplete_week(closes, timestamps)
        assert len(result_closes) == 2

    def test_keeps_complete_week_candle(self):
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        last_monday = now - timedelta(days=now.weekday()) - timedelta(weeks=1)
        last_ts = int(last_monday.replace(hour=0, minute=0, second=0).timestamp())
        prev_ts = last_ts - 604800
        closes = [100.0, 101.0]
        timestamps = [prev_ts, last_ts]
        result_closes, _ = _strip_incomplete_week(closes, timestamps)
        assert len(result_closes) == 2


class TestAggregateToMonthly:
    def test_groups_by_calendar_month(self):
        from datetime import datetime, timezone
        ts_jan = int(datetime(2025, 1, 20, tzinfo=timezone.utc).timestamp())
        ts_feb = int(datetime(2025, 2, 10, tzinfo=timezone.utc).timestamp())
        closes = [102.0, 201.0]
        result = _aggregate_to_monthly(closes, [ts_jan, ts_feb])
        assert len(result) == 2
        assert result == [102.0, 201.0]


@pytest.fixture
def mock_handler_deps():
    with patch("src.worker.app._process_batch") as mock_process, \
         patch("src.worker.app.storage.put_json") as mock_put, \
         patch("src.worker.app.storage.list_objects") as mock_list, \
         patch("src.worker.app._aggregate_and_finalize") as mock_agg, \
         patch("src.worker.app.storage.invalidate_cache") as mock_inv:
        mock_list.return_value = []
        yield mock_process, mock_put, mock_list, mock_agg, mock_inv


class TestLambdaHandler:
    def _sqs_event(self, messages: list[dict]) -> dict:
        return {"Records": [{"body": json.dumps(msg)} for msg in messages]}

    @patch.dict("os.environ", {"BUCKET_NAME": "test-bucket"})
    def test_processes_sqs_message(self, mock_handler_deps):
        mock_process, mock_put, _, _, _ = mock_handler_deps
        mock_process.return_value = BatchResult()
        event = self._sqs_event([{"runId": "r", "batchIndex": 0, "totalBatches": 3, "symbols": ["A"]}])
        lambda_handler(event, None)
        mock_process.assert_called_once()
        assert mock_put.call_count >= 1

    @patch.dict("os.environ", {"BUCKET_NAME": "test-bucket"})
    def test_empty_records(self, mock_handler_deps):
        mock_process, _, _, _, _ = mock_handler_deps
        result = lambda_handler({"Records": []}, None)
        assert result["statusCode"] == 200
        mock_process.assert_not_called()


class TestWriteBatchResults:
    def test_writes_to_correct_key(self, mock_storage):
        mock_s3, _ = mock_storage
        from src.worker.app import _write_batch_results
        _write_batch_results("mybucket", "run", 5, 50, 0, BatchResult())
        mock_s3.put_object.assert_called_once()
        assert "batch-005.json" in mock_s3.put_object.call_args[1]["Key"]


class TestAggregateResults:
    def test_merges_all_batches(self, mock_agg_deps):
        mock_read, mock_put, _ = mock_agg_deps
        mock_read.side_effect = [EMPTY_BATCH, EMPTY_BATCH]
        from src.worker.app import _aggregate_and_finalize
        _aggregate_and_finalize("b", "r", 2)
        # 10 keys in _RESULT_FILES + errorDetails + stats + dated files
        assert mock_put.call_count >= 10

    def test_writes_latest_json(self, mock_agg_deps):
        mock_read, mock_put, _ = mock_agg_deps
        mock_read.return_value = EMPTY_BATCH
        from src.worker.app import _aggregate_and_finalize
        _aggregate_and_finalize("b", "r", 1)
        assert any("results/latest.json" == call[0][1] for call in mock_put.call_args_list)


class TestUpdateManifest:
    def test_creates_manifest_when_none_exists(self, mock_agg_deps):
        mock_read, mock_put, _ = mock_agg_deps
        mock_read.return_value = None
        _update_manifest("b", "2026-03-14")
        mock_put.assert_called_once()

    def test_prepends_new_date(self, mock_agg_deps):
        mock_read, mock_put, _ = mock_agg_deps
        mock_read.return_value = {"weeks": ["2026-03-07"]}
        _update_manifest("b", "2026-03-14")
        assert mock_put.call_args[0][2]["weeks"] == ["2026-03-14", "2026-03-07"]


class TestDeleteSnapshot:
    def test_deletes_all_snapshot_files(self, mock_storage):
        mock_s3, _ = mock_storage
        _delete_snapshot("b", "d")
        assert mock_s3.delete_object.call_count == 9


class TestComputeMiscStats:
    def test_empty_stats_returns_empty(self):
        assert _compute_misc_stats([]) == {}

    def test_pct_within_5_of_high(self):
        stats = [{"highPct": -3.0}, {"highPct": -10.0}, {"highPct": 0.0}]
        result = _compute_misc_stats(stats)
        assert result["pctWithin5OfHigh"] == 66.7


class TestAggregateResultsStats:
    def test_writes_latest_stats_json(self, mock_agg_deps):
        mock_read, mock_put, _ = mock_agg_deps
        mock_read.return_value = EMPTY_BATCH
        from src.worker.app import _aggregate_and_finalize
        _aggregate_and_finalize("b", "r", 1)
        put_keys = [call[0][1] for call in mock_put.call_args_list]
        assert "results/latest-stats.json" in put_keys
