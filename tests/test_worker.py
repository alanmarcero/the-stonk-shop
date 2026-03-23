import json
from unittest.mock import patch, MagicMock

from src.worker.app import (
    lambda_handler,
    _process_batch,
    _aggregate_results,
    _aggregate_to_monthly,
    _aggregate_to_quarterly,
    _strip_incomplete_week,
    _invalidate_cache,
    _update_manifest,
    _delete_snapshot,
    _write_batch_results,
    _write_errors,
    _compute_misc_stats,
    BatchResult,
    MAX_WEEKLY_SNAPSHOTS,
)


# -- Shared test data --

# Crossover: 3 weeks below then cross above
CROSSOVER_CLOSES = [100.0, 102.0, 104.0, 106.0, 108.0, 100.0, 101.0, 101.0, 106.0]

# Steady below: 3 weeks below EMA (no crossover)
BELOW_CLOSES = [100.0, 102.0, 104.0, 106.0, 108.0, 100.0, 101.0, 101.0]

# Uptrend: all above EMA (no crossover, no below)
UPTREND_CLOSES = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]

# Crossdown: 4 weeks above then cross below
CROSSDOWN_CLOSES = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 40.0]

# Reusable empty batch for aggregation tests
EMPTY_BATCH = {"symbolsProcessed": 10, "errors": 0, "errorDetails": [], "crossovers": [], "crossdowns": [], "dayBelow": [], "weekBelow": [], "dayAbove": [], "weekAbove": [], "monthCrossovers": [], "monthCrossdowns": [], "monthBelow": [], "monthAbove": [], "quarterCrossovers": [], "quarterCrossdowns": [], "quarterBelow": [], "quarterAbove": [], "stats": []}


def _timestamps_for(closes):
    return list(range(len(closes)))


class TestProcessBatch:

    def setup_method(self):
        self._yahoo_patcher = patch("src.worker.app.yahoo")
        self._time_patcher = patch("src.worker.app.time")
        self._stats_patcher = patch("src.worker.app.stats")
        self.mock_yahoo = self._yahoo_patcher.start()
        self.mock_time = self._time_patcher.start()
        self.mock_stats = self._stats_patcher.start()
        self.mock_yahoo.fetch_quarterly_candles.return_value = None
        self.mock_yahoo.fetch_stats_candles.return_value = None
        self.mock_yahoo.fetch_forward_pe.return_value = (None, None)
        self.mock_stats.compute_stats.return_value = None

    def teardown_method(self):
        self._stats_patcher.stop()
        self._time_patcher.stop()
        self._yahoo_patcher.stop()

    def test_crossover_detected(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES))

        result = _process_batch(["TEST"])

        assert len(result.crossovers) == 1
        assert result.crossovers[0]["symbol"] == "TEST"
        assert result.crossovers[0]["close"] == 106.0
        assert result.crossovers[0]["weeksBelow"] == 3
        assert result.crossovers[0]["pctAbove"] > 0
        assert len(result.errors) == 0

    def test_crossover_output_fields(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES))

        result = _process_batch(["AAPL"])

        entry = result.crossovers[0]
        assert set(entry.keys()) == {"symbol", "close", "ema", "pctAbove", "weeksBelow"}
        assert isinstance(entry["close"], float)
        assert isinstance(entry["ema"], float)
        assert isinstance(entry["pctAbove"], float)
        assert isinstance(entry["weeksBelow"], int)

    def test_crossover_ema_rounded_to_4_decimals(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES))

        result = _process_batch(["X"])

        ema_str = str(result.crossovers[0]["ema"])
        decimals = ema_str.split(".")[-1] if "." in ema_str else ""
        assert len(decimals) <= 4

    def test_crossover_pct_above_rounded_to_2_decimals(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES))

        result = _process_batch(["X"])

        pct = result.crossovers[0]["pctAbove"]
        assert pct == round(pct, 2)

    def test_week_below_detected_with_minimum_weeks(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES))

        result = _process_batch(["TEST"])

        assert len(result.week_below) == 1
        assert result.week_below[0]["symbol"] == "TEST"
        assert result.week_below[0]["count"] == 3
        assert result.week_below[0]["pctBelow"] > 0

    def test_week_below_output_fields(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES))

        result = _process_batch(["X"])

        entry = result.week_below[0]
        assert set(entry.keys()) == {"symbol", "close", "ema", "pctBelow", "count"}

    def test_week_below_not_detected_under_threshold(self):
        closes = [50.0, 52.0, 54.0, 56.0, 58.0, 56.0, 53.0]
        self.mock_yahoo.fetch_quarterly_candles.return_value = (closes, _timestamps_for(closes))

        result = _process_batch(["TEST"])

        assert len(result.week_below) == 0

    def test_week_below_two_weeks_not_detected(self):
        closes = [100.0, 102.0, 104.0, 106.0, 108.0, 100.0, 101.0]
        self.mock_yahoo.fetch_quarterly_candles.return_value = (closes, _timestamps_for(closes))

        result = _process_batch(["TEST"])

        assert len(result.week_below) == 0

    def test_uptrend_no_crossover_no_below(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (UPTREND_CLOSES, _timestamps_for(UPTREND_CLOSES))

        result = _process_batch(["BULL"])

        assert len(result.crossovers) == 0
        assert len(result.week_below) == 0
        assert len(result.errors) == 0

    def test_fetch_failure_records_error(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = None

        result = _process_batch(["FAIL"])

        assert len(result.crossovers) == 0
        assert len(result.week_below) == 0
        assert len(result.day_above) == 0
        assert len(result.week_above) == 0
        assert len(result.errors) == 1
        assert result.errors[0]["symbol"] == "FAIL"
        assert "error" in result.errors[0]

    def test_insufficient_data_skipped_no_error(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = ([100.0, 101.0, 102.0], [1, 2, 3])

        result = _process_batch(["SHORT"])

        assert len(result.crossovers) == 0
        assert len(result.week_below) == 0
        assert len(result.errors) == 0

    def test_multiple_symbols_rate_limited(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = ([50.0] * 10, list(range(10)))

        _process_batch(["A", "B", "C"])

        assert self.mock_time.sleep.call_count == 2
        self.mock_time.sleep.assert_called_with(1)

    def test_single_symbol_no_sleep(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = ([50.0] * 10, list(range(10)))

        _process_batch(["ONLY"])

        self.mock_time.sleep.assert_not_called()

    def test_mixed_success_and_failure(self):
        def weekly_side_effect(symbol):
            if symbol == "FAIL":
                return None
            return (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES))

        self.mock_yahoo.fetch_quarterly_candles.side_effect = weekly_side_effect

        result = _process_batch(["OK", "FAIL", "OK2"])

        assert len(result.crossovers) == 2
        assert len(result.errors) == 1
        assert result.errors[0]["symbol"] == "FAIL"

    def test_empty_batch(self):
        result = _process_batch([])

        assert result.crossovers == []
        assert result.crossdowns == []
        assert result.day_below == []
        assert result.week_below == []
        assert result.day_above == []
        assert result.week_above == []
        assert result.errors == []
        self.mock_yahoo.fetch_quarterly_candles.assert_not_called()
        self.mock_yahoo.fetch_stats_candles.assert_not_called()

    def test_all_failures(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = None

        result = _process_batch(["A", "B", "C"])

        assert len(result.crossovers) == 0
        assert len(result.week_below) == 0
        assert len(result.day_above) == 0
        assert len(result.week_above) == 0
        assert len(result.errors) == 3

    def test_day_above_detected(self):
        self.mock_yahoo.fetch_stats_candles.return_value = (UPTREND_CLOSES, _timestamps_for(UPTREND_CLOSES))
        self.mock_yahoo.fetch_quarterly_candles.return_value = (UPTREND_CLOSES, _timestamps_for(UPTREND_CLOSES))

        result = _process_batch(["BULL"])

        assert len(result.day_above) == 1
        assert result.day_above[0]["symbol"] == "BULL"
        assert result.day_above[0]["count"] == 6
        assert result.day_above[0]["pctAbove"] > 0

    def test_day_above_output_fields(self):
        self.mock_yahoo.fetch_stats_candles.return_value = (UPTREND_CLOSES, _timestamps_for(UPTREND_CLOSES))
        self.mock_yahoo.fetch_quarterly_candles.return_value = (UPTREND_CLOSES, _timestamps_for(UPTREND_CLOSES))

        result = _process_batch(["X"])

        entry = result.day_above[0]
        assert set(entry.keys()) == {"symbol", "close", "ema", "pctAbove", "count"}
        assert isinstance(entry["count"], int)

    def test_week_above_detected(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (UPTREND_CLOSES, _timestamps_for(UPTREND_CLOSES))

        result = _process_batch(["BULL"])

        assert len(result.week_above) == 1
        assert result.week_above[0]["symbol"] == "BULL"
        assert result.week_above[0]["count"] == 6
        assert result.week_above[0]["pctAbove"] > 0

    def test_week_above_output_fields(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (UPTREND_CLOSES, _timestamps_for(UPTREND_CLOSES))

        result = _process_batch(["X"])

        entry = result.week_above[0]
        assert set(entry.keys()) == {"symbol", "close", "ema", "pctAbove", "count"}

    def test_daily_fail_still_processes_weekly(self):
        self.mock_yahoo.fetch_stats_candles.return_value = None
        self.mock_yahoo.fetch_quarterly_candles.return_value = (CROSSOVER_CLOSES, _timestamps_for(CROSSOVER_CLOSES))

        result = _process_batch(["TEST"])

        assert len(result.crossovers) == 1
        assert len(result.errors) == 0

    def test_weekly_fail_still_processes_daily(self):
        self.mock_yahoo.fetch_stats_candles.return_value = (UPTREND_CLOSES, _timestamps_for(UPTREND_CLOSES))
        self.mock_yahoo.fetch_quarterly_candles.return_value = None

        result = _process_batch(["TEST"])

        assert len(result.day_above) == 1
        assert len(result.errors) == 0

    def test_all_fetches_fail_records_error(self):
        self.mock_yahoo.fetch_stats_candles.return_value = None
        self.mock_yahoo.fetch_quarterly_candles.return_value = None

        result = _process_batch(["FAIL"])

        assert len(result.errors) == 1
        assert result.errors[0]["symbol"] == "FAIL"

    def test_below_ema_not_in_above_lists(self):
        self.mock_yahoo.fetch_stats_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES))
        self.mock_yahoo.fetch_quarterly_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES))

        result = _process_batch(["BEAR"])

        assert len(result.day_above) == 0
        assert len(result.week_above) == 0

    def test_crossdown_detected(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (CROSSDOWN_CLOSES, _timestamps_for(CROSSDOWN_CLOSES))

        result = _process_batch(["TEST"])

        assert len(result.crossdowns) == 1
        assert result.crossdowns[0]["symbol"] == "TEST"
        assert result.crossdowns[0]["weeksAbove"] == 4
        assert result.crossdowns[0]["pctBelow"] >= 0

    def test_crossdown_output_fields(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = (CROSSDOWN_CLOSES, _timestamps_for(CROSSDOWN_CLOSES))

        result = _process_batch(["X"])

        entry = result.crossdowns[0]
        assert set(entry.keys()) == {"symbol", "close", "ema", "pctBelow", "weeksAbove"}
        assert isinstance(entry["close"], float)
        assert isinstance(entry["ema"], float)
        assert isinstance(entry["pctBelow"], float)
        assert isinstance(entry["weeksAbove"], int)

    def test_day_below_detected(self):
        self.mock_yahoo.fetch_stats_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES))
        self.mock_yahoo.fetch_quarterly_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES))

        result = _process_batch(["BEAR"])

        assert len(result.day_below) == 1
        assert result.day_below[0]["symbol"] == "BEAR"
        assert result.day_below[0]["count"] == 3
        assert result.day_below[0]["pctBelow"] > 0

    def test_day_below_output_fields(self):
        self.mock_yahoo.fetch_stats_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES))
        self.mock_yahoo.fetch_quarterly_candles.return_value = (BELOW_CLOSES, _timestamps_for(BELOW_CLOSES))

        result = _process_batch(["X"])

        entry = result.day_below[0]
        assert set(entry.keys()) == {"symbol", "close", "ema", "pctBelow", "count"}

    def test_monthly_crossover_detected(self):
        # _aggregate_to_monthly turns weekly closes into monthly last-closes
        # We need enough monthly data points (>= 6) for crossover detection
        from datetime import datetime, timezone
        monthly_closes = [100.0, 102.0, 104.0, 106.0, 108.0, 100.0, 101.0, 101.0, 106.0]
        timestamps = [
            int(datetime(2024, m, 15, tzinfo=timezone.utc).timestamp())
            for m in range(1, 10)
        ]
        self.mock_yahoo.fetch_quarterly_candles.return_value = (monthly_closes, timestamps)

        result = _process_batch(["TEST"])

        assert len(result.month_crossovers) == 1
        assert result.month_crossovers[0]["symbol"] == "TEST"
        assert "monthsBelow" in result.month_crossovers[0]

    def test_monthly_crossdown_detected(self):
        from datetime import datetime, timezone
        monthly_closes = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 40.0]
        timestamps = [
            int(datetime(2024, m, 15, tzinfo=timezone.utc).timestamp())
            for m in range(1, 10)
        ]
        self.mock_yahoo.fetch_quarterly_candles.return_value = (monthly_closes, timestamps)

        result = _process_batch(["TEST"])

        assert len(result.month_crossdowns) == 1
        assert result.month_crossdowns[0]["symbol"] == "TEST"
        assert "monthsAbove" in result.month_crossdowns[0]

    def test_quarterly_crossover_detected(self):
        from datetime import datetime, timezone
        # Each close maps to a different quarter
        quarterly_closes = [100.0, 102.0, 104.0, 106.0, 108.0, 100.0, 101.0, 101.0, 106.0]
        timestamps = [
            int(datetime(2022 + i // 4, (i % 4) * 3 + 2, 15, tzinfo=timezone.utc).timestamp())
            for i in range(9)
        ]
        self.mock_yahoo.fetch_quarterly_candles.return_value = (quarterly_closes, timestamps)

        result = _process_batch(["TEST"])

        assert len(result.quarter_crossovers) == 1
        assert result.quarter_crossovers[0]["symbol"] == "TEST"
        assert "quartersBelow" in result.quarter_crossovers[0]

    def test_stats_computed_when_stats_candles_available(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = ([50.0] * 10, list(range(10)))
        self.mock_yahoo.fetch_stats_candles.return_value = ([100.0, 105.0], [1000, 2000])
        self.mock_yahoo.fetch_forward_pe.return_value = (18.5, {"Q3'25": 18.5})
        self.mock_stats.compute_stats.return_value = {"close": 105.0, "ytdPct": 5.0}

        result = _process_batch(["AAPL"])

        assert len(result.stats_data) == 1
        assert result.stats_data[0]["symbol"] == "AAPL"
        assert result.stats_data[0]["close"] == 105.0

    def test_voo_includes_election_and_inauguration_returns(self):
        self.mock_yahoo.fetch_quarterly_candles.return_value = ([50.0] * 10, list(range(10)))
        self.mock_yahoo.fetch_stats_candles.return_value = ([100.0, 105.0], [1000, 2000])
        self.mock_yahoo.fetch_forward_pe.return_value = (None, None)
        self.mock_stats.compute_stats.return_value = {"close": 105.0}
        self.mock_stats.compute_return_since.side_effect = [12.5, -3.0]

        result = _process_batch(["VOO"])

        assert len(result.stats_data) == 1
        assert result.stats_data[0]["spxSinceElection"] == 12.5
        assert result.stats_data[0]["spxSinceInauguration"] == -3.0
        assert self.mock_stats.compute_return_since.call_count == 2


class TestStripIncompleteWeek:

    def test_strips_current_week_candle(self):
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        # Monday of current week
        current_monday = now - timedelta(days=now.weekday())
        current_ts = int(current_monday.replace(hour=0, minute=0, second=0).timestamp())
        # Last week Monday
        last_monday_ts = int((current_monday - timedelta(weeks=1)).replace(hour=0, minute=0, second=0).timestamp())

        closes = [100.0, 101.0, 102.0]
        timestamps = [last_monday_ts - 604800, last_monday_ts, current_ts]

        result_closes, result_ts = _strip_incomplete_week(closes, timestamps)

        assert len(result_closes) == 2
        assert len(result_ts) == 2
        assert result_closes[-1] == 101.0

    def test_keeps_complete_week_candle(self):
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        # Last week Monday
        last_monday = now - timedelta(days=now.weekday()) - timedelta(weeks=1)
        last_ts = int(last_monday.replace(hour=0, minute=0, second=0).timestamp())
        prev_ts = last_ts - 604800

        closes = [100.0, 101.0]
        timestamps = [prev_ts, last_ts]

        result_closes, result_ts = _strip_incomplete_week(closes, timestamps)

        assert len(result_closes) == 2

    def test_empty_input(self):
        closes, timestamps = _strip_incomplete_week([], [])
        assert closes == []
        assert timestamps == []


class TestAggregateToMonthly:

    def test_groups_by_calendar_month(self):
        # 3 weeks in Jan 2026, 2 weeks in Feb 2026
        closes = [100.0, 101.0, 102.0, 103.0, 104.0]
        timestamps = [
            1735689600,  # 2025-01-01
            1736294400,  # 2025-01-08
            1736899200,  # 2025-01-15
            1738108800,  # 2025-01-29 -> still Jan
            1738713600,  # 2025-02-05
        ]
        # Actually let's use clear month boundaries
        from datetime import datetime, timezone
        ts_jan_1 = int(datetime(2025, 1, 6, tzinfo=timezone.utc).timestamp())
        ts_jan_2 = int(datetime(2025, 1, 13, tzinfo=timezone.utc).timestamp())
        ts_jan_3 = int(datetime(2025, 1, 20, tzinfo=timezone.utc).timestamp())
        ts_feb_1 = int(datetime(2025, 2, 3, tzinfo=timezone.utc).timestamp())
        ts_feb_2 = int(datetime(2025, 2, 10, tzinfo=timezone.utc).timestamp())

        closes = [100.0, 101.0, 102.0, 200.0, 201.0]
        timestamps = [ts_jan_1, ts_jan_2, ts_jan_3, ts_feb_1, ts_feb_2]

        result = _aggregate_to_monthly(closes, timestamps)

        assert len(result) == 2
        assert result[0] == 102.0  # last close in Jan
        assert result[1] == 201.0  # last close in Feb

    def test_single_month(self):
        from datetime import datetime, timezone
        ts1 = int(datetime(2025, 3, 3, tzinfo=timezone.utc).timestamp())
        ts2 = int(datetime(2025, 3, 10, tzinfo=timezone.utc).timestamp())

        result = _aggregate_to_monthly([50.0, 55.0], [ts1, ts2])

        assert result == [55.0]

    def test_empty_input(self):
        assert _aggregate_to_monthly([], []) == []

    def test_three_months_produces_three_entries(self):
        from datetime import datetime, timezone
        # One candle per month in Jan, Feb, Mar
        ts_jan = int(datetime(2025, 1, 13, tzinfo=timezone.utc).timestamp())
        ts_feb = int(datetime(2025, 2, 10, tzinfo=timezone.utc).timestamp())
        ts_mar = int(datetime(2025, 3, 10, tzinfo=timezone.utc).timestamp())

        result = _aggregate_to_monthly([100.0, 110.0, 120.0], [ts_jan, ts_feb, ts_mar])

        assert len(result) == 3
        assert result == [100.0, 110.0, 120.0]


class TestAggregateToQuarterly:

    def test_groups_by_calendar_quarter(self):
        from datetime import datetime, timezone
        # Q1: Jan + Mar, Q2: Apr
        ts_jan = int(datetime(2025, 1, 6, tzinfo=timezone.utc).timestamp())
        ts_mar = int(datetime(2025, 3, 24, tzinfo=timezone.utc).timestamp())
        ts_apr = int(datetime(2025, 4, 7, tzinfo=timezone.utc).timestamp())

        closes = [100.0, 102.0, 200.0]
        timestamps = [ts_jan, ts_mar, ts_apr]

        result = _aggregate_to_quarterly(closes, timestamps)

        assert len(result) == 2
        assert result[0] == 102.0  # last close in Q1
        assert result[1] == 200.0  # last close in Q2

    def test_single_quarter(self):
        from datetime import datetime, timezone
        ts1 = int(datetime(2025, 1, 6, tzinfo=timezone.utc).timestamp())
        ts2 = int(datetime(2025, 2, 3, tzinfo=timezone.utc).timestamp())
        ts3 = int(datetime(2025, 3, 3, tzinfo=timezone.utc).timestamp())

        result = _aggregate_to_quarterly([50.0, 55.0, 60.0], [ts1, ts2, ts3])

        assert result == [60.0]

    def test_empty_input(self):
        assert _aggregate_to_quarterly([], []) == []

    def test_four_quarters_produces_four_entries(self):
        from datetime import datetime, timezone
        # One candle per quarter across 4 quarters
        ts_q1 = int(datetime(2025, 2, 10, tzinfo=timezone.utc).timestamp())
        ts_q2 = int(datetime(2025, 5, 12, tzinfo=timezone.utc).timestamp())
        ts_q3 = int(datetime(2025, 8, 11, tzinfo=timezone.utc).timestamp())
        ts_q4 = int(datetime(2025, 11, 10, tzinfo=timezone.utc).timestamp())

        result = _aggregate_to_quarterly(
            [100.0, 200.0, 300.0, 400.0],
            [ts_q1, ts_q2, ts_q3, ts_q4],
        )

        assert len(result) == 4
        assert result == [100.0, 200.0, 300.0, 400.0]

    def test_last_close_wins_per_quarter(self):
        from datetime import datetime, timezone
        ts1 = int(datetime(2025, 1, 6, tzinfo=timezone.utc).timestamp())
        ts2 = int(datetime(2025, 1, 13, tzinfo=timezone.utc).timestamp())
        ts3 = int(datetime(2025, 2, 3, tzinfo=timezone.utc).timestamp())
        ts4 = int(datetime(2025, 3, 31, tzinfo=timezone.utc).timestamp())

        closes = [100.0, 110.0, 120.0, 130.0]
        timestamps = [ts1, ts2, ts3, ts4]

        result = _aggregate_to_quarterly(closes, timestamps)

        assert len(result) == 1
        assert result[0] == 130.0  # last close in Q1


class TestWriteBatchResults:

    @patch("src.worker.app.s3")
    def test_writes_to_correct_key(self, mock_s3):
        _write_batch_results("mybucket", "2026-02-22", 5, 50, 2, BatchResult())

        mock_s3.put_object.assert_called_once()
        kwargs = mock_s3.put_object.call_args[1]
        assert kwargs["Bucket"] == "mybucket"
        assert kwargs["Key"] == "batches/2026-02-22/batch-005.json"

    @patch("src.worker.app.s3")
    def test_batch_index_zero_padded(self, mock_s3):
        _write_batch_results("b", "r", 0, 10, 0, BatchResult())
        assert "batch-000.json" in mock_s3.put_object.call_args[1]["Key"]

        _write_batch_results("b", "r", 99, 10, 0, BatchResult())
        assert "batch-099.json" in mock_s3.put_object.call_args[1]["Key"]

        _write_batch_results("b", "r", 159, 10, 0, BatchResult())
        assert "batch-159.json" in mock_s3.put_object.call_args[1]["Key"]

    @patch("src.worker.app.s3")
    def test_body_contains_all_fields(self, mock_s3):
        crossovers = [{"symbol": "AAPL", "weeksBelow": 3}]
        crossdowns = [{"symbol": "NVDA", "weeksAbove": 5}]
        day_below = [{"symbol": "AMZN", "count": 2}]
        week_below = [{"symbol": "MSFT", "count": 4}]
        day_above = [{"symbol": "GOOG", "count": 5}]
        week_above = [{"symbol": "TSLA", "count": 3}]

        month_crossovers = [{"symbol": "META", "monthsBelow": 2}]
        month_crossdowns = [{"symbol": "NFLX", "monthsAbove": 3}]
        month_below = [{"symbol": "INTC", "count": 4}]
        month_above = [{"symbol": "AMD", "count": 2}]
        quarter_crossovers = [{"symbol": "V", "quartersBelow": 3}]
        quarter_crossdowns = [{"symbol": "MA", "quartersAbove": 2}]
        quarter_below = [{"symbol": "PYPL", "count": 5}]
        quarter_above = [{"symbol": "SQ", "count": 3}]
        stats_data = [{"symbol": "AAPL", "close": 195.5, "ytdPct": 12.34}]
        error_details = [{"symbol": "BAD", "error": "fail"}]

        batch = BatchResult(
            crossovers=crossovers, crossdowns=crossdowns,
            day_below=day_below, week_below=week_below,
            day_above=day_above, week_above=week_above,
            month_crossovers=month_crossovers, month_crossdowns=month_crossdowns,
            month_below=month_below, month_above=month_above,
            quarter_crossovers=quarter_crossovers, quarter_crossdowns=quarter_crossdowns,
            quarter_below=quarter_below, quarter_above=quarter_above,
            stats_data=stats_data, errors=error_details,
        )
        _write_batch_results("b", "r", 0, 50, 2, batch)

        body = json.loads(mock_s3.put_object.call_args[1]["Body"])
        assert body["batchIndex"] == 0
        assert body["symbolsProcessed"] == 50
        assert body["errors"] == 2
        assert body["errorDetails"] == error_details
        assert body["crossovers"] == crossovers
        assert body["crossdowns"] == crossdowns
        assert body["dayBelow"] == day_below
        assert body["weekBelow"] == week_below
        assert body["dayAbove"] == day_above
        assert body["weekAbove"] == week_above
        assert body["monthCrossovers"] == month_crossovers
        assert body["monthCrossdowns"] == month_crossdowns
        assert body["monthBelow"] == month_below
        assert body["monthAbove"] == month_above
        assert body["quarterCrossovers"] == quarter_crossovers
        assert body["quarterCrossdowns"] == quarter_crossdowns
        assert body["quarterBelow"] == quarter_below
        assert body["quarterAbove"] == quarter_above
        assert body["stats"] == stats_data


class TestWriteErrors:

    @patch("src.worker.app.s3")
    def test_writes_to_correct_key(self, mock_s3):
        errors = [{"symbol": "BAD", "error": "fail"}]

        _write_errors("mybucket", "2026-02-22", 3, errors)

        kwargs = mock_s3.put_object.call_args[1]
        assert kwargs["Bucket"] == "mybucket"
        assert kwargs["Key"] == "logs/2026-02-22/errors-003.json"

    @patch("src.worker.app.s3")
    def test_body_is_error_list(self, mock_s3):
        errors = [{"symbol": "A", "error": "x"}, {"symbol": "B", "error": "y"}]

        _write_errors("b", "r", 0, errors)

        body = json.loads(mock_s3.put_object.call_args[1]["Body"])
        assert len(body) == 2
        assert body[0]["symbol"] == "A"


class TestLambdaHandler:

    def setup_method(self):
        self._env_patcher = patch.dict("os.environ", {"BUCKET_NAME": "test-bucket"})
        self._process_patcher = patch("src.worker.app._process_batch")
        self._write_patcher = patch("src.worker.app._write_batch_results")
        self._errors_patcher = patch("src.worker.app._write_errors")
        self._agg_patcher = patch("src.worker.app._aggregate_results")
        self._inv_patcher = patch("src.worker.app._invalidate_cache")
        self._env_patcher.start()
        self.mock_process = self._process_patcher.start()
        self.mock_write = self._write_patcher.start()
        self.mock_errors = self._errors_patcher.start()
        self.mock_agg = self._agg_patcher.start()
        self.mock_invalidate = self._inv_patcher.start()

    def teardown_method(self):
        self._inv_patcher.stop()
        self._agg_patcher.stop()
        self._errors_patcher.stop()
        self._write_patcher.stop()
        self._process_patcher.stop()
        self._env_patcher.stop()

    def _sqs_event(self, messages: list[dict]) -> dict:
        return {
            "Records": [
                {"body": json.dumps(msg)} for msg in messages
            ]
        }

    def test_processes_sqs_message(self):
        self.mock_process.return_value = BatchResult()
        event = self._sqs_event([{
            "runId": "2026-02-22",
            "batchIndex": 0,
            "totalBatches": 3,
            "symbols": ["AAPL", "MSFT"],
        }])

        result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        self.mock_process.assert_called_once_with(["AAPL", "MSFT"], [])
        self.mock_write.assert_called_once()

    def test_passes_vix_spikes_to_process_batch(self):
        self.mock_process.return_value = BatchResult()
        vix_spikes = [{"dateString": "3/10/25", "timestamp": 1000, "vixClose": 25.0}]
        event = self._sqs_event([{
            "runId": "2026-02-22",
            "batchIndex": 0,
            "totalBatches": 1,
            "symbols": ["AAPL"],
            "vixSpikes": vix_spikes,
        }])

        lambda_handler(event, None)

        self.mock_process.assert_called_once_with(["AAPL"], vix_spikes)

    def test_last_batch_triggers_aggregation(self):
        self.mock_process.return_value = BatchResult()
        event = self._sqs_event([{
            "runId": "2026-02-22",
            "batchIndex": 2,
            "totalBatches": 3,

            "symbols": ["AAPL"],
        }])

        lambda_handler(event, None)

        self.mock_agg.assert_called_once_with("test-bucket", "2026-02-22", 3)

    def test_non_last_batch_skips_aggregation(self):
        self.mock_process.return_value = BatchResult()
        event = self._sqs_event([{
            "runId": "2026-02-22",
            "batchIndex": 0,
            "totalBatches": 3,

            "symbols": ["AAPL"],
        }])

        lambda_handler(event, None)

        self.mock_agg.assert_not_called()

    def test_errors_written_when_present(self):
        self.mock_process.return_value = BatchResult(errors=[{"symbol": "BAD", "error": "fail"}])
        event = self._sqs_event([{
            "runId": "2026-02-22",
            "batchIndex": 0,
            "totalBatches": 1,

            "symbols": ["BAD"],
        }])

        lambda_handler(event, None)

        self.mock_errors.assert_called_once()

    def test_errors_not_written_when_empty(self):
        self.mock_process.return_value = BatchResult()
        event = self._sqs_event([{
            "runId": "2026-02-22",
            "batchIndex": 0,
            "totalBatches": 1,

            "symbols": ["AAPL"],
        }])

        lambda_handler(event, None)

        self.mock_errors.assert_not_called()

    def test_empty_records(self):
        result = lambda_handler({"Records": []}, None)

        assert result["statusCode"] == 200
        self.mock_process.assert_not_called()

    def test_single_batch_total_triggers_aggregation(self):
        self.mock_process.return_value = BatchResult()
        event = self._sqs_event([{
            "runId": "2026-02-22",
            "batchIndex": 0,
            "totalBatches": 1,

            "symbols": ["AAPL"],
        }])

        lambda_handler(event, None)

        self.mock_agg.assert_called_once()

    def test_last_batch_invalidates_cache(self):
        self.mock_process.return_value = BatchResult()
        event = self._sqs_event([{
            "runId": "2026-02-22",
            "batchIndex": 2,
            "totalBatches": 3,
            "symbols": ["AAPL"],
        }])

        lambda_handler(event, None)

        self.mock_invalidate.assert_called_once()

    def test_non_last_batch_skips_invalidation(self):
        self.mock_process.return_value = BatchResult()
        event = self._sqs_event([{
            "runId": "2026-02-22",
            "batchIndex": 0,
            "totalBatches": 3,
            "symbols": ["AAPL"],
        }])

        lambda_handler(event, None)

        self.mock_invalidate.assert_not_called()


class TestInvalidateCache:

    @patch("src.worker.app.cloudfront")
    @patch.dict("os.environ", {"DISTRIBUTION_ID": "E1234567890"})
    def test_creates_invalidation(self, mock_cf):
        _invalidate_cache()

        mock_cf.create_invalidation.assert_called_once()
        args = mock_cf.create_invalidation.call_args[1]
        assert args["DistributionId"] == "E1234567890"
        assert args["InvalidationBatch"]["Paths"]["Items"] == ["/results/*"]

    @patch("src.worker.app.cloudfront")
    @patch.dict("os.environ", {}, clear=True)
    def test_skips_when_no_distribution_id(self, mock_cf):
        _invalidate_cache()

        mock_cf.create_invalidation.assert_not_called()


class TestAggregateResults:

    def setup_method(self):
        self._read_patcher = patch("src.worker.app._read_json")
        self._put_patcher = patch("src.worker.app._put_json")
        self._manifest_patcher = patch("src.worker.app._update_manifest")
        self.mock_read = self._read_patcher.start()
        self.mock_put = self._put_patcher.start()
        self.mock_manifest = self._manifest_patcher.start()

    def teardown_method(self):
        self._manifest_patcher.stop()
        self._put_patcher.stop()
        self._read_patcher.stop()

    def test_merges_all_batches(self):
        self.mock_read.side_effect = [
            {
                "symbolsProcessed": 50,
                "errors": 1,
                "crossovers": [{"symbol": "AAPL", "weeksBelow": 5}],
                "dayBelow": [{"symbol": "X", "count": 4}],
                "weekBelow": [],
                "dayAbove": [{"symbol": "GOOG", "count": 3}],
                "weekAbove": [],
            },
            {
                "symbolsProcessed": 50,
                "errors": 0,
                "crossovers": [{"symbol": "MSFT", "weeksBelow": 3}],
                "dayBelow": [],
                "weekBelow": [{"symbol": "Y", "count": 5}],
                "dayAbove": [],
                "weekAbove": [{"symbol": "TSLA", "count": 2}],
            },
        ]

        _aggregate_results("test-bucket", "2026-02-22", 2)

        assert self.mock_put.call_count == 19
        latest_data = self.mock_put.call_args_list[0][0][2]
        assert latest_data["symbolsScanned"] == 100
        assert latest_data["errors"] == 1
        assert len(latest_data["crossovers"]) == 2

    def test_crossovers_sorted_by_weeks_below_descending(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0,
             "crossovers": [{"symbol": "LOW", "weeksBelow": 3}]},
            {"symbolsProcessed": 50, "errors": 0,
             "crossovers": [{"symbol": "HIGH", "weeksBelow": 8}]},
        ]

        _aggregate_results("b", "r", 2)

        crossovers = self.mock_put.call_args_list[0][0][2]["crossovers"]
        assert crossovers[0]["symbol"] == "HIGH"
        assert crossovers[1]["symbol"] == "LOW"

    def test_week_below_sorted_by_count_descending(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0, "crossovers": [],
             "weekBelow": [{"symbol": "LOW", "count": 3}]},
            {"symbolsProcessed": 50, "errors": 0, "crossovers": [],
             "weekBelow": [{"symbol": "HIGH", "count": 10}]},
        ]

        _aggregate_results("b", "r", 2)

        below_data = self.mock_put.call_args_list[2][0][2]
        assert below_data["weekBelow"][0]["symbol"] == "HIGH"
        assert below_data["weekBelow"][1]["symbol"] == "LOW"

    def test_handles_missing_batch_file(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0, "crossovers": []},
            None,
        ]

        _aggregate_results("b", "r", 2)

        latest_data = self.mock_put.call_args_list[0][0][2]
        assert latest_data["symbolsScanned"] == 50

    def test_all_batches_missing(self):
        self.mock_read.return_value = None

        _aggregate_results("b", "r", 3)

        latest_data = self.mock_put.call_args_list[0][0][2]
        assert latest_data["symbolsScanned"] == 0
        assert latest_data["errors"] == 0
        assert latest_data["crossovers"] == []

    def test_writes_latest_json(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 1)

        assert self.mock_put.call_args_list[0][0][1] == "results/latest.json"

    def test_writes_latest_below_json(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 1)

        assert self.mock_put.call_args_list[2][0][1] == "results/latest-below.json"

    def test_writes_latest_above_json(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 1)

        assert self.mock_put.call_args_list[3][0][1] == "results/latest-above.json"

    def test_writes_archive_with_date(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("b", "2026-02-22", 1)

        archive_key = self.mock_put.call_args_list[9][0][1]
        assert archive_key.startswith("results/")
        assert ".json" in archive_key

    def test_latest_json_has_required_fields(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("b", "r", 1)

        data = self.mock_put.call_args_list[0][0][2]
        assert set(data.keys()) == {"scanDate", "scanTime", "symbolsScanned", "errors", "crossovers"}

    def test_latest_below_json_has_required_fields(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("b", "r", 1)

        data = self.mock_put.call_args_list[2][0][2]
        assert set(data.keys()) == {"scanDate", "scanTime", "symbolsScanned", "errors", "dayBelow", "weekBelow"}

    def test_latest_above_json_has_required_fields(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("b", "r", 1)

        data = self.mock_put.call_args_list[3][0][2]
        assert set(data.keys()) == {"scanDate", "scanTime", "symbolsScanned", "errors", "dayAbove", "weekAbove"}

    def test_reads_correct_batch_keys(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 3)

        read_calls = self.mock_read.call_args_list
        assert read_calls[0][0] == ("mybucket", "batches/2026-02-22/batch-000.json")
        assert read_calls[1][0] == ("mybucket", "batches/2026-02-22/batch-001.json")
        assert read_calls[2][0] == ("mybucket", "batches/2026-02-22/batch-002.json")

    def test_error_counts_accumulate(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 3, "crossovers": []},
            {"symbolsProcessed": 50, "errors": 7, "crossovers": []},
            {"symbolsProcessed": 50, "errors": 5, "crossovers": []},
        ]

        _aggregate_results("b", "r", 3)

        latest_data = self.mock_put.call_args_list[0][0][2]
        assert latest_data["errors"] == 15
        assert latest_data["symbolsScanned"] == 150

    def test_day_above_merged_and_sorted_by_count_descending(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0, "crossovers": [],
             "dayAbove": [{"symbol": "LOW", "count": 2}], "weekAbove": []},
            {"symbolsProcessed": 50, "errors": 0, "crossovers": [],
             "dayAbove": [{"symbol": "HIGH", "count": 10}], "weekAbove": []},
        ]

        _aggregate_results("b", "r", 2)

        above_data = self.mock_put.call_args_list[3][0][2]
        assert len(above_data["dayAbove"]) == 2
        assert above_data["dayAbove"][0]["symbol"] == "HIGH"
        assert above_data["dayAbove"][1]["symbol"] == "LOW"

    def test_week_above_merged_and_sorted_by_count_descending(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0, "crossovers": [],
             "dayAbove": [], "weekAbove": [{"symbol": "LOW", "count": 1}]},
            {"symbolsProcessed": 50, "errors": 0, "crossovers": [],
             "dayAbove": [], "weekAbove": [{"symbol": "HIGH", "count": 8}]},
        ]

        _aggregate_results("b", "r", 2)

        above_data = self.mock_put.call_args_list[3][0][2]
        assert len(above_data["weekAbove"]) == 2
        assert above_data["weekAbove"][0]["symbol"] == "HIGH"
        assert above_data["weekAbove"][1]["symbol"] == "LOW"

    def test_backward_compat_missing_above_keys(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0, "crossovers": []},
        ]

        _aggregate_results("b", "r", 1)

        above_data = self.mock_put.call_args_list[3][0][2]
        assert above_data["dayAbove"] == []
        assert above_data["weekAbove"] == []

    def test_crossdowns_merged_and_sorted_by_weeks_above_descending(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0, "crossovers": [], "crossdowns": [{"symbol": "LOW", "weeksAbove": 3}]},
            {"symbolsProcessed": 50, "errors": 0, "crossovers": [], "crossdowns": [{"symbol": "HIGH", "weeksAbove": 8}]},
        ]

        _aggregate_results("b", "r", 2)

        crossdown_data = self.mock_put.call_args_list[1][0][2]
        assert crossdown_data["crossdowns"][0]["symbol"] == "HIGH"
        assert crossdown_data["crossdowns"][1]["symbol"] == "LOW"

    def test_writes_latest_crossdown_json(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 1)

        assert self.mock_put.call_args_list[1][0][1] == "results/latest-crossdown.json"

    def test_latest_crossdown_json_has_required_fields(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("b", "r", 1)

        data = self.mock_put.call_args_list[1][0][2]
        assert set(data.keys()) == {"scanDate", "scanTime", "symbolsScanned", "errors", "crossdowns"}

    def test_backward_compat_missing_crossdowns_key(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0, "crossovers": []},
        ]

        _aggregate_results("b", "r", 1)

        crossdown_data = self.mock_put.call_args_list[1][0][2]
        assert crossdown_data["crossdowns"] == []

    def test_writes_latest_monthly_json(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 1)

        assert self.mock_put.call_args_list[4][0][1] == "results/latest-monthly.json"

    def test_writes_latest_monthly_below_above_json(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 1)

        assert self.mock_put.call_args_list[5][0][1] == "results/latest-monthly-below-above.json"

    def test_latest_monthly_json_has_required_fields(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("b", "r", 1)

        data = self.mock_put.call_args_list[4][0][2]
        assert set(data.keys()) == {"scanDate", "scanTime", "symbolsScanned", "errors", "monthCrossovers", "monthCrossdowns"}

    def test_latest_monthly_below_above_json_has_required_fields(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("b", "r", 1)

        data = self.mock_put.call_args_list[5][0][2]
        assert set(data.keys()) == {"scanDate", "scanTime", "symbolsScanned", "errors", "monthBelow", "monthAbove"}

    def test_month_crossovers_sorted_by_months_below_descending(self):
        self.mock_read.side_effect = [
            {**EMPTY_BATCH, "monthCrossovers": [{"symbol": "LOW", "monthsBelow": 2}]},
            {**EMPTY_BATCH, "monthCrossovers": [{"symbol": "HIGH", "monthsBelow": 5}]},
        ]

        _aggregate_results("b", "r", 2)

        monthly_data = self.mock_put.call_args_list[4][0][2]
        assert monthly_data["monthCrossovers"][0]["symbol"] == "HIGH"
        assert monthly_data["monthCrossovers"][1]["symbol"] == "LOW"

    def test_month_crossdowns_sorted_by_months_above_descending(self):
        self.mock_read.side_effect = [
            {**EMPTY_BATCH, "monthCrossdowns": [{"symbol": "LOW", "monthsAbove": 1}]},
            {**EMPTY_BATCH, "monthCrossdowns": [{"symbol": "HIGH", "monthsAbove": 4}]},
        ]

        _aggregate_results("b", "r", 2)

        monthly_data = self.mock_put.call_args_list[4][0][2]
        assert monthly_data["monthCrossdowns"][0]["symbol"] == "HIGH"
        assert monthly_data["monthCrossdowns"][1]["symbol"] == "LOW"

    def test_backward_compat_missing_monthly_keys(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0, "crossovers": []},
        ]

        _aggregate_results("b", "r", 1)

        monthly_data = self.mock_put.call_args_list[4][0][2]
        assert monthly_data["monthCrossovers"] == []
        assert monthly_data["monthCrossdowns"] == []
        monthly_ba_data = self.mock_put.call_args_list[5][0][2]
        assert monthly_ba_data["monthBelow"] == []
        assert monthly_ba_data["monthAbove"] == []

    def test_writes_latest_quarterly_json(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 1)

        assert self.mock_put.call_args_list[6][0][1] == "results/latest-quarterly.json"

    def test_writes_latest_quarterly_below_above_json(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 1)

        assert self.mock_put.call_args_list[7][0][1] == "results/latest-quarterly-below-above.json"

    def test_latest_quarterly_json_has_required_fields(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("b", "r", 1)

        data = self.mock_put.call_args_list[6][0][2]
        assert set(data.keys()) == {"scanDate", "scanTime", "symbolsScanned", "errors", "quarterCrossovers", "quarterCrossdowns"}

    def test_latest_quarterly_below_above_json_has_required_fields(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("b", "r", 1)

        data = self.mock_put.call_args_list[7][0][2]
        assert set(data.keys()) == {"scanDate", "scanTime", "symbolsScanned", "errors", "quarterBelow", "quarterAbove"}

    def test_quarter_crossovers_sorted_by_quarters_below_descending(self):
        self.mock_read.side_effect = [
            {**EMPTY_BATCH, "quarterCrossovers": [{"symbol": "LOW", "quartersBelow": 2}]},
            {**EMPTY_BATCH, "quarterCrossovers": [{"symbol": "HIGH", "quartersBelow": 5}]},
        ]

        _aggregate_results("b", "r", 2)

        quarterly_data = self.mock_put.call_args_list[6][0][2]
        assert quarterly_data["quarterCrossovers"][0]["symbol"] == "HIGH"
        assert quarterly_data["quarterCrossovers"][1]["symbol"] == "LOW"

    def test_quarter_crossdowns_sorted_by_quarters_above_descending(self):
        self.mock_read.side_effect = [
            {**EMPTY_BATCH, "quarterCrossdowns": [{"symbol": "LOW", "quartersAbove": 1}]},
            {**EMPTY_BATCH, "quarterCrossdowns": [{"symbol": "HIGH", "quartersAbove": 4}]},
        ]

        _aggregate_results("b", "r", 2)

        quarterly_data = self.mock_put.call_args_list[6][0][2]
        assert quarterly_data["quarterCrossdowns"][0]["symbol"] == "HIGH"
        assert quarterly_data["quarterCrossdowns"][1]["symbol"] == "LOW"

    def test_backward_compat_missing_quarterly_keys(self):
        self.mock_read.side_effect = [
            {"symbolsProcessed": 50, "errors": 0, "crossovers": []},
        ]

        _aggregate_results("b", "r", 1)

        quarterly_data = self.mock_put.call_args_list[6][0][2]
        assert quarterly_data["quarterCrossovers"] == []
        assert quarterly_data["quarterCrossdowns"] == []
        quarterly_ba_data = self.mock_put.call_args_list[7][0][2]
        assert quarterly_ba_data["quarterBelow"] == []
        assert quarterly_ba_data["quarterAbove"] == []

    def test_aggregate_writes_all_dated_files(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-03-14", 1)

        put_keys = {call[0][1] for call in self.mock_put.call_args_list}
        # Dated files use datetime.now(), so extract the actual date used
        dated_keys = {k for k in put_keys if "latest" not in k and "manifest" not in k}
        assert len(dated_keys) == 9
        # Verify all expected suffixes are present
        suffixes_found = {k.split("/")[-1].split(".", 1)[0].split("-", 4)[-1] if "-" in k.split("/")[-1].split("-", 4)[-1] else "" for k in dated_keys}
        assert "crossdown" in {k.split("/")[-1].replace(".json", "").split("-", 4)[-1] for k in dated_keys if "crossdown" in k}
        assert any("below.json" in k and "monthly" not in k and "quarterly" not in k for k in dated_keys)
        assert any("above.json" in k and "monthly" not in k and "quarterly" not in k for k in dated_keys)
        assert any("monthly.json" in k for k in dated_keys)
        assert any("monthly-below-above.json" in k for k in dated_keys)
        assert any("quarterly.json" in k for k in dated_keys)
        assert any("quarterly-below-above.json" in k for k in dated_keys)
        assert any("stats.json" in k for k in dated_keys)

    def test_aggregate_calls_update_manifest(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-03-14", 1)

        self.mock_manifest.assert_called_once()
        args = self.mock_manifest.call_args[0]
        assert args[0] == "mybucket"


class TestUpdateManifest:

    def setup_method(self):
        self._read_patcher = patch("src.worker.app._read_json")
        self._put_patcher = patch("src.worker.app._put_json")
        self._delete_patcher = patch("src.worker.app._delete_snapshot")
        self.mock_read = self._read_patcher.start()
        self.mock_put = self._put_patcher.start()
        self.mock_delete = self._delete_patcher.start()

    def teardown_method(self):
        self._delete_patcher.stop()
        self._put_patcher.stop()
        self._read_patcher.stop()

    def test_creates_manifest_when_none_exists(self):
        self.mock_read.return_value = None

        _update_manifest("mybucket", "2026-03-14")

        self.mock_put.assert_called_once_with(
            "mybucket", "results/manifest.json", {"weeks": ["2026-03-14"]}
        )

    def test_prepends_new_date(self):
        self.mock_read.return_value = {"weeks": ["2026-03-07", "2026-02-28"]}

        _update_manifest("mybucket", "2026-03-14")

        written = self.mock_put.call_args[0][2]
        assert written["weeks"] == ["2026-03-14", "2026-03-07", "2026-02-28"]

    def test_avoids_duplicate_date(self):
        self.mock_read.return_value = {"weeks": ["2026-03-14", "2026-03-07"]}

        _update_manifest("mybucket", "2026-03-14")

        written = self.mock_put.call_args[0][2]
        assert written["weeks"] == ["2026-03-14", "2026-03-07"]
        assert written["weeks"].count("2026-03-14") == 1

    def test_duplicate_date_moves_to_front(self):
        self.mock_read.return_value = {"weeks": ["2026-03-07", "2026-03-14", "2026-02-28"]}

        _update_manifest("mybucket", "2026-03-14")

        written = self.mock_put.call_args[0][2]
        assert written["weeks"][0] == "2026-03-14"
        assert len(written["weeks"]) == 3

    def test_trims_to_max_snapshots(self):
        existing = [f"2026-03-{13 - i:02d}" for i in range(6)]
        self.mock_read.return_value = {"weeks": existing}

        _update_manifest("mybucket", "2026-03-14")

        written = self.mock_put.call_args[0][2]
        assert len(written["weeks"]) == MAX_WEEKLY_SNAPSHOTS
        assert written["weeks"][0] == "2026-03-14"

    def test_deletes_trimmed_snapshots(self):
        existing = [f"2026-03-{13 - i:02d}" for i in range(6)]
        # existing = ["2026-03-13", "2026-03-12", ..., "2026-03-08"]
        self.mock_read.return_value = {"weeks": existing}

        _update_manifest("mybucket", "2026-03-14")

        # "2026-03-08" should be trimmed (7th entry after prepend)
        self.mock_delete.assert_called_once_with("mybucket", "2026-03-08")

    def test_deletes_multiple_trimmed_snapshots(self):
        existing = [f"2026-03-{13 - i:02d}" for i in range(7)]
        # existing has 7 entries; after prepend we have 8, trim to 6
        self.mock_read.return_value = {"weeks": existing}

        _update_manifest("mybucket", "2026-03-14")

        assert self.mock_delete.call_count == 2

    def test_no_deletes_when_under_max(self):
        self.mock_read.return_value = {"weeks": ["2026-03-07"]}

        _update_manifest("mybucket", "2026-03-14")

        self.mock_delete.assert_not_called()

    def test_reads_manifest_from_correct_key(self):
        self.mock_read.return_value = None

        _update_manifest("mybucket", "2026-03-14")

        self.mock_read.assert_called_once_with("mybucket", "results/manifest.json")

    def test_empty_weeks_in_manifest(self):
        self.mock_read.return_value = {"weeks": []}

        _update_manifest("mybucket", "2026-03-14")

        written = self.mock_put.call_args[0][2]
        assert written["weeks"] == ["2026-03-14"]


class TestDeleteSnapshot:

    @patch("src.worker.app.s3")
    def test_deletes_all_snapshot_files(self, mock_s3):
        _delete_snapshot("mybucket", "2026-03-08")

        assert mock_s3.delete_object.call_count == 9
        deleted_keys = [call[1]["Key"] for call in mock_s3.delete_object.call_args_list]
        assert "results/2026-03-08.json" in deleted_keys
        assert "results/2026-03-08-crossdown.json" in deleted_keys
        assert "results/2026-03-08-below.json" in deleted_keys
        assert "results/2026-03-08-above.json" in deleted_keys
        assert "results/2026-03-08-monthly.json" in deleted_keys
        assert "results/2026-03-08-monthly-below-above.json" in deleted_keys
        assert "results/2026-03-08-quarterly.json" in deleted_keys
        assert "results/2026-03-08-quarterly-below-above.json" in deleted_keys
        assert "results/2026-03-08-stats.json" in deleted_keys

    @patch("src.worker.app.s3")
    def test_uses_correct_bucket(self, mock_s3):
        _delete_snapshot("my-special-bucket", "2026-03-08")

        assert mock_s3.delete_object.call_count == 9
        buckets = {call[1]["Bucket"] for call in mock_s3.delete_object.call_args_list}
        assert buckets == {"my-special-bucket"}

    @patch("src.worker.app.s3")
    def test_continues_on_delete_error(self, mock_s3):
        mock_s3.delete_object.side_effect = [Exception("fail"), None, None, None, None, None, None, None, None]

        _delete_snapshot("mybucket", "2026-03-08")

        assert mock_s3.delete_object.call_count == 9


class TestComputeMiscStats:

    def test_empty_stats_returns_empty(self):
        assert _compute_misc_stats([]) == {}

    def test_pct_within_5_of_high(self):
        stats = [
            {"highPct": -3.0, "ytdPct": 5.0},
            {"highPct": -10.0, "ytdPct": -2.0},
            {"highPct": 0.0, "ytdPct": 10.0},
        ]
        result = _compute_misc_stats(stats)
        # 2 out of 3 are >= -5
        assert result["pctWithin5OfHigh"] == 66.7

    def test_pct_positive_ytd(self):
        stats = [
            {"ytdPct": 5.0},
            {"ytdPct": -2.0},
            {"ytdPct": 0.0},
        ]
        result = _compute_misc_stats(stats)
        # 2 out of 3 (5.0 and 0.0 are >= 0)
        assert result["pctPositiveYTD"] == 66.7

    def test_avg_ytd(self):
        stats = [
            {"ytdPct": 10.0},
            {"ytdPct": -4.0},
            {"ytdPct": 6.0},
        ]
        result = _compute_misc_stats(stats)
        assert result["avgYTD"] == 4.0

    def test_forward_pe_avg_and_median(self):
        stats = [
            {"forwardPE": 10.0},
            {"forwardPE": 20.0},
            {"forwardPE": 30.0},
        ]
        result = _compute_misc_stats(stats)
        assert result["avgForwardPE"] == 20.0
        assert result["medianForwardPE"] == 20.0

    def test_forward_pe_even_count_median(self):
        stats = [
            {"forwardPE": 10.0},
            {"forwardPE": 20.0},
            {"forwardPE": 30.0},
            {"forwardPE": 40.0},
        ]
        result = _compute_misc_stats(stats)
        assert result["medianForwardPE"] == 25.0

    def test_no_forward_pe_omits_pe_fields(self):
        stats = [{"ytdPct": 5.0}]
        result = _compute_misc_stats(stats)
        assert "avgForwardPE" not in result
        assert "medianForwardPE" not in result

    def test_no_ytd_omits_ytd_fields(self):
        stats = [{"highPct": -3.0}]
        result = _compute_misc_stats(stats)
        assert "pctPositiveYTD" not in result
        assert "avgYTD" not in result

    def test_ema_percentages(self):
        stats = [{"ytdPct": 5.0}]
        result = _compute_misc_stats(stats, week_above_count=60, total_symbols=100)
        assert result["pctAbove5wkEMA"] == 60.0
        assert result["pctBelow5wkEMA"] == 40.0

    def test_ema_percentages_zero_total(self):
        stats = [{"ytdPct": 5.0}]
        result = _compute_misc_stats(stats, week_above_count=0, total_symbols=0)
        assert "pctAbove5wkEMA" not in result
        assert "pctBelow5wkEMA" not in result

    def test_pct_above_200d_sma(self):
        stats = [
            {"pctSma200d": 5.0},
            {"pctSma200d": -3.0},
            {"pctSma200d": 0.0},
        ]
        result = _compute_misc_stats(stats)
        # 2 out of 3 are >= 0
        assert result["pctAbove200dSMA"] == 66.7

    def test_pct_above_200w_sma(self):
        stats = [
            {"pctSma200w": 10.0},
            {"pctSma200w": -5.0},
            {"pctSma200w": 2.0},
            {"pctSma200w": -1.0},
        ]
        result = _compute_misc_stats(stats)
        # 2 out of 4 are >= 0
        assert result["pctAbove200wSMA"] == 50.0

    def test_no_sma_data_omits_sma_fields(self):
        stats = [{"ytdPct": 5.0}]
        result = _compute_misc_stats(stats)
        assert "pctAbove200dSMA" not in result
        assert "pctAbove200wSMA" not in result

    def test_spx_returns_from_voo(self):
        stats = [
            {"symbol": "AAPL", "ytdPct": 5.0},
            {"symbol": "VOO", "ytdPct": 8.0, "spxSinceElection": 12.5, "spxSinceInauguration": -3.0},
        ]
        result = _compute_misc_stats(stats)
        assert result["spxSinceElection"] == 12.5
        assert result["spxSinceInauguration"] == -3.0

    def test_spx_returns_absent_without_voo(self):
        stats = [{"symbol": "AAPL", "ytdPct": 5.0}]
        result = _compute_misc_stats(stats)
        assert "spxSinceElection" not in result
        assert "spxSinceInauguration" not in result

    def test_forward_pe_median_odd_count(self):
        stats = [
            {"forwardPE": 10.0},
            {"forwardPE": 20.0},
            {"forwardPE": 50.0},
            {"forwardPE": 30.0},
            {"forwardPE": 40.0},
        ]
        result = _compute_misc_stats(stats)
        # Sorted: [10, 20, 30, 40, 50] -> median is 30
        assert result["medianForwardPE"] == 30.0


class TestAggregateResultsStats:

    def setup_method(self):
        self._read_patcher = patch("src.worker.app._read_json")
        self._put_patcher = patch("src.worker.app._put_json")
        self._manifest_patcher = patch("src.worker.app._update_manifest")
        self.mock_read = self._read_patcher.start()
        self.mock_put = self._put_patcher.start()
        self.mock_manifest = self._manifest_patcher.start()

    def teardown_method(self):
        self._manifest_patcher.stop()
        self._put_patcher.stop()
        self._read_patcher.stop()

    def test_stats_merged_and_sorted_by_symbol(self):
        self.mock_read.side_effect = [
            {**EMPTY_BATCH, "stats": [{"symbol": "MSFT", "close": 400.0}]},
            {**EMPTY_BATCH, "stats": [{"symbol": "AAPL", "close": 200.0}]},
        ]

        _aggregate_results("b", "r", 2)

        stats_put = next(
            call for call in self.mock_put.call_args_list
            if "latest-stats.json" in call[0][1]
        )
        stats_data = stats_put[0][2]["stats"]
        assert len(stats_data) == 2
        assert stats_data[0]["symbol"] == "AAPL"
        assert stats_data[1]["symbol"] == "MSFT"

    def test_writes_latest_stats_json(self):
        self.mock_read.return_value = EMPTY_BATCH

        _aggregate_results("mybucket", "2026-02-22", 1)

        put_keys = [call[0][1] for call in self.mock_put.call_args_list]
        assert "results/latest-stats.json" in put_keys

    def test_latest_stats_json_has_misc_field(self):
        self.mock_read.return_value = {
            **EMPTY_BATCH,
            "stats": [{"symbol": "A", "ytdPct": 5.0, "highPct": -2.0}],
        }

        _aggregate_results("b", "r", 1)

        stats_put = next(
            call for call in self.mock_put.call_args_list
            if "latest-stats.json" in call[0][1]
        )
        assert "misc" in stats_put[0][2]
        assert "stats" in stats_put[0][2]

    def test_writes_latest_errors_json(self):
        self.mock_read.return_value = {
            **EMPTY_BATCH,
            "errorDetails": [{"symbol": "BAD", "error": "fail"}],
        }

        _aggregate_results("mybucket", "2026-02-22", 1)

        put_keys = [call[0][1] for call in self.mock_put.call_args_list]
        assert "results/latest-errors.json" in put_keys

    def test_errors_sorted_by_symbol(self):
        self.mock_read.side_effect = [
            {**EMPTY_BATCH, "errorDetails": [{"symbol": "ZZZ", "error": "a"}]},
            {**EMPTY_BATCH, "errorDetails": [{"symbol": "AAA", "error": "b"}]},
        ]

        _aggregate_results("b", "r", 2)

        errors_put = next(
            call for call in self.mock_put.call_args_list
            if "latest-errors.json" in call[0][1]
        )
        errors = errors_put[0][2]["errorDetails"]
        assert len(errors) == 2
        assert errors[0]["symbol"] == "AAA"
        assert errors[1]["symbol"] == "ZZZ"

    def test_day_below_merged_and_sorted_by_count_descending(self):
        self.mock_read.side_effect = [
            {**EMPTY_BATCH, "dayBelow": [{"symbol": "LOW", "count": 2}]},
            {**EMPTY_BATCH, "dayBelow": [{"symbol": "HIGH", "count": 10}]},
        ]

        _aggregate_results("b", "r", 2)

        below_data = self.mock_put.call_args_list[2][0][2]
        assert len(below_data["dayBelow"]) == 2
        assert below_data["dayBelow"][0]["symbol"] == "HIGH"
        assert below_data["dayBelow"][1]["symbol"] == "LOW"

    def test_month_below_merged_and_sorted(self):
        self.mock_read.side_effect = [
            {**EMPTY_BATCH, "monthBelow": [{"symbol": "LOW", "count": 1}]},
            {**EMPTY_BATCH, "monthBelow": [{"symbol": "HIGH", "count": 5}]},
        ]

        _aggregate_results("b", "r", 2)

        monthly_ba = self.mock_put.call_args_list[5][0][2]
        assert monthly_ba["monthBelow"][0]["symbol"] == "HIGH"
        assert monthly_ba["monthBelow"][1]["symbol"] == "LOW"

    def test_quarter_below_merged_and_sorted(self):
        self.mock_read.side_effect = [
            {**EMPTY_BATCH, "quarterBelow": [{"symbol": "LOW", "count": 1}]},
            {**EMPTY_BATCH, "quarterBelow": [{"symbol": "HIGH", "count": 5}]},
        ]

        _aggregate_results("b", "r", 2)

        quarterly_ba = self.mock_put.call_args_list[7][0][2]
        assert quarterly_ba["quarterBelow"][0]["symbol"] == "HIGH"
        assert quarterly_ba["quarterBelow"][1]["symbol"] == "LOW"
