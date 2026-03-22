from datetime import datetime, timezone

from src.worker.swing import analyze


def _ts(year, month, day):
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp())


class TestAnalyze:

    def test_empty_returns_none(self):
        assert analyze([], []) is None

    def test_mismatched_lengths_returns_none(self):
        assert analyze([100.0, 200.0], [1000]) is None

    def test_steady_uptrend_finds_breakdown(self):
        # 100→120 is a 20% rise from initial, so 100 is recorded as breakdown
        closes = [100.0, 105.0, 110.0, 115.0, 120.0]
        timestamps = [_ts(2024, 1, i + 1) for i in range(5)]

        result = analyze(closes, timestamps)

        assert result is not None
        assert result["breakdownPrice"] == 100.0

    def test_breakout_detected(self):
        # Rise to 100, drop 15% to 85, then recover
        closes = [50.0, 70.0, 90.0, 100.0, 85.0, 90.0, 95.0]
        timestamps = [_ts(2024, 1, i + 1) for i in range(7)]

        result = analyze(closes, timestamps)

        assert result is not None
        assert result["breakoutPrice"] == 100.0
        assert "breakoutDate" in result
        assert "breakoutPct" in result
        # Current close is 95, breakoutPrice is 100 => -5%
        assert result["breakoutPct"] == -5.0

    def test_breakdown_detected(self):
        # Drop to 50, rise to 60 (20% from 50 triggers), then rise to 70 (17% from 60 triggers)
        # Last significant low is 60 (the reset min after first trigger)
        closes = [100.0, 80.0, 60.0, 50.0, 60.0, 70.0]
        timestamps = [_ts(2024, 1, i + 1) for i in range(6)]

        result = analyze(closes, timestamps)

        assert result is not None
        assert result["breakdownPrice"] == 60.0
        assert "breakdownDate" in result
        assert "breakdownPct" in result

    def test_both_breakout_and_breakdown(self):
        # Up to 100, drop to 85 (breakout at 100), up to 90, drop to 50, rise to 60 (breakdown at 50)
        closes = [50.0, 70.0, 100.0, 85.0, 90.0, 50.0, 60.0]
        timestamps = [_ts(2024, 1, i + 1) for i in range(7)]

        result = analyze(closes, timestamps)

        assert result is not None
        assert "breakoutPrice" in result
        assert "breakdownPrice" in result

    def test_breakout_uses_last_significant_high(self):
        # Two significant declines: high at 100 then drop, high at 200 then drop
        closes = [50.0, 100.0, 85.0, 120.0, 200.0, 170.0, 175.0]
        timestamps = [_ts(2024, 1, i + 1) for i in range(7)]

        result = analyze(closes, timestamps)

        assert result is not None
        assert result["breakoutPrice"] == 200.0

    def test_breakdown_uses_last_significant_low(self):
        # Two significant rises: low at 50 then rise, low at 30 then rise
        closes = [100.0, 50.0, 60.0, 70.0, 30.0, 40.0]
        timestamps = [_ts(2024, 1, i + 1) for i in range(6)]

        result = analyze(closes, timestamps)

        assert result is not None
        assert result["breakdownPrice"] == 30.0

    def test_threshold_boundary_not_triggered(self):
        # Drop of exactly 9% — should NOT trigger
        closes = [100.0, 91.0]
        timestamps = [_ts(2024, 1, 1), _ts(2024, 1, 2)]

        result = analyze(closes, timestamps)

        assert result is None

    def test_threshold_boundary_triggered(self):
        # Drop of exactly 10% — should trigger
        closes = [100.0, 90.0, 95.0]
        timestamps = [_ts(2024, 1, 1), _ts(2024, 1, 2), _ts(2024, 1, 3)]

        result = analyze(closes, timestamps)

        assert result is not None
        assert result["breakoutPrice"] == 100.0

    def test_single_close_returns_none(self):
        result = analyze([100.0], [_ts(2024, 1, 1)])
        assert result is None

    def test_date_formatted_correctly(self):
        closes = [100.0, 90.0, 95.0]
        timestamps = [_ts(2024, 3, 15), _ts(2024, 4, 1), _ts(2024, 4, 15)]

        result = analyze(closes, timestamps)

        assert result is not None
        # Date should be 3/15/24 format
        assert result["breakoutDate"] == "3/15/24"

    def test_breakdown_skips_zero_close(self):
        closes = [0.0, 0.0, 10.0]
        timestamps = [_ts(2024, 1, 1), _ts(2024, 1, 2), _ts(2024, 1, 3)]

        result = analyze(closes, timestamps)

        # Should not crash on zero division
        assert result is None or "breakdownPrice" not in result or result["breakdownPrice"] > 0


