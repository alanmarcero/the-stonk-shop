from datetime import datetime, timezone

from src.worker.quarterly import compute_quarterly_changes


def _ts(year, month, day):
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp())


class TestComputeQuarterlyChanges:

    def test_empty_returns_none(self):
        assert compute_quarterly_changes([], []) is None

    def test_single_close_returns_none(self):
        assert compute_quarterly_changes([100.0], [_ts(2024, 1, 1)]) is None

    def test_mismatched_lengths_returns_none(self):
        assert compute_quarterly_changes([100.0, 200.0], [1000]) is None

    def test_no_quarter_end_months_returns_none(self):
        # Only data in Jan and Feb — no quarter-end months
        closes = [100.0, 105.0, 110.0]
        timestamps = [_ts(2024, 1, 15), _ts(2024, 2, 1), _ts(2024, 2, 15)]

        result = compute_quarterly_changes(closes, timestamps)

        assert result is None

    def test_single_quarter_end(self):
        closes = [100.0, 105.0, 110.0, 108.0, 112.0]
        timestamps = [
            _ts(2024, 3, 1), _ts(2024, 3, 15), _ts(2024, 3, 28),
            _ts(2024, 4, 1), _ts(2024, 4, 15),
        ]

        result = compute_quarterly_changes(closes, timestamps)

        assert result is not None
        assert "sinceQuarter" in result
        assert "duringQuarter" in result
        assert "Q1'24" in result["sinceQuarter"]
        # Since: (112 - 110) / 110 * 100 = 1.82
        assert result["sinceQuarter"]["Q1'24"] == 1.82
        # During: only one quarter end, so no during entry
        assert "Q1'24" not in result["duringQuarter"]

    def test_two_quarter_ends(self):
        closes = [100.0, 110.0, 105.0, 120.0, 130.0]
        timestamps = [
            _ts(2024, 3, 29), _ts(2024, 6, 28),
            _ts(2024, 7, 1),
            _ts(2024, 9, 30), _ts(2024, 10, 15),
        ]

        result = compute_quarterly_changes(closes, timestamps)

        assert result is not None
        assert "Q1'24" in result["sinceQuarter"]
        assert "Q3'24" in result["sinceQuarter"]
        # During Q3: (120 - 110) / 110 * 100 = 9.09
        assert "Q3'24" in result["duringQuarter"]
        assert result["duringQuarter"]["Q3'24"] == 9.09

    def test_uses_last_close_in_quarter_month(self):
        # Multiple closes in March — should use the last one
        closes = [95.0, 100.0, 105.0, 110.0, 115.0]
        timestamps = [
            _ts(2024, 3, 1), _ts(2024, 3, 15), _ts(2024, 3, 28),
            _ts(2024, 4, 1), _ts(2024, 4, 15),
        ]

        result = compute_quarterly_changes(closes, timestamps)

        # Q1'24 close should be 105.0 (last close in March)
        # Since: (115 - 105) / 105 * 100 = 9.52
        assert result["sinceQuarter"]["Q1'24"] == 9.52

    def test_excludes_current_quarter_end_month(self):
        # If current date is in a quarter-end month, don't include that quarter
        closes = [100.0, 105.0, 110.0]
        timestamps = [
            _ts(2024, 3, 28), _ts(2024, 6, 15), _ts(2024, 6, 28),
        ]

        result = compute_quarterly_changes(closes, timestamps)

        assert result is not None
        assert "Q1'24" in result["sinceQuarter"]
        # Q2'24 should NOT be present since we're still in June
        assert "Q2'24" not in result["sinceQuarter"]

    def test_quarter_labels_format(self):
        closes = [100.0, 110.0, 105.0, 120.0, 115.0, 130.0, 125.0, 140.0, 135.0]
        timestamps = [
            _ts(2024, 3, 29), _ts(2024, 6, 28), _ts(2024, 7, 1),
            _ts(2024, 9, 30), _ts(2024, 10, 1),
            _ts(2024, 12, 31), _ts(2025, 1, 2),
            _ts(2025, 3, 28), _ts(2025, 4, 1),
        ]

        result = compute_quarterly_changes(closes, timestamps)

        labels = list(result["sinceQuarter"].keys())
        assert "Q1'24" in labels
        assert "Q2'24" in labels
        assert "Q3'24" in labels
        assert "Q4'24" in labels
        assert "Q1'25" in labels

    def test_negative_since_quarter(self):
        closes = [100.0, 110.0, 100.0]
        timestamps = [_ts(2024, 3, 29), _ts(2024, 6, 28), _ts(2024, 7, 15)]

        result = compute_quarterly_changes(closes, timestamps)

        # Q2: since = (100 - 110) / 110 * 100 = -9.09
        assert result["sinceQuarter"]["Q2'24"] < 0

    def test_negative_during_quarter(self):
        closes = [110.0, 100.0, 95.0]
        timestamps = [_ts(2024, 3, 29), _ts(2024, 6, 28), _ts(2024, 7, 15)]

        result = compute_quarterly_changes(closes, timestamps)

        # During Q2: (100 - 110) / 110 * 100 = -9.09
        assert result["duringQuarter"]["Q2'24"] == -9.09
