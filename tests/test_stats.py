from datetime import datetime, timezone
import pytest

from src.worker.stats import (
    compute_ytd_pct,
    compute_highest_close_pct,
    compute_lowest_close_pct,
    compute_return_since,
    compute_stats,
)


def _ts(year, month, day):
    """Helper to create a UTC timestamp for a given date."""
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp())


class TestComputeYtdPct:

    def test_known_dec31_close(self):
        # Arrangement
        closes = [100.0, 101.0, 105.0, 112.0]
        timestamps = [
            _ts(2025, 12, 30),
            _ts(2025, 12, 31),
            _ts(2026, 1, 2),
            _ts(2026, 1, 3),
        ]

        # Action
        result = compute_ytd_pct(closes, timestamps)

        # Assertion: (112 - 101) / 101 * 100 = 10.89
        assert result == 10.89

    def test_negative_ytd(self):
        closes = [200.0, 190.0]
        timestamps = [_ts(2025, 12, 31), _ts(2026, 1, 2)]

        result = compute_ytd_pct(closes, timestamps)

        assert result == -5.0

    def test_ipo_after_dec31_returns_none(self):
        closes = [50.0, 55.0, 60.0]
        timestamps = [_ts(2026, 2, 1), _ts(2026, 2, 2), _ts(2026, 2, 3)]

        result = compute_ytd_pct(closes, timestamps)

        assert result is None

    def test_insufficient_data_returns_none(self):
        assert compute_ytd_pct([100.0], [_ts(2026, 1, 2)]) is None

    def test_empty_returns_none(self):
        assert compute_ytd_pct([], []) is None

    def test_uses_last_close_of_prev_year(self):
        closes = [90.0, 95.0, 100.0, 110.0]
        timestamps = [
            _ts(2025, 12, 29),
            _ts(2025, 12, 30),
            _ts(2025, 12, 31),
            _ts(2026, 1, 2),
        ]

        result = compute_ytd_pct(closes, timestamps)

        # (110 - 100) / 100 * 100 = 10.0
        assert result == 10.0


class TestComputeHighestClosePct:

    def test_at_high(self):
        closes = [90.0, 95.0, 100.0]

        pct, high = compute_highest_close_pct(closes)

        assert high == 100.0
        assert pct == 0.0

    def test_below_high(self):
        closes = [90.0, 100.0, 95.0]

        pct, high = compute_highest_close_pct(closes)

        assert high == 100.0
        assert pct == -5.0

    def test_empty_returns_none(self):
        assert compute_highest_close_pct([]) is None


class TestComputeLowestClosePct:

    def test_uses_last_252_closes(self):
        # 300 closes, min in the first 48 (outside 252 window)
        closes = [10.0] + [200.0] * 299
        closes[100] = 50.0  # inside 252 window

        _, low = compute_lowest_close_pct(closes)

        assert low == 50.0

    def test_fewer_than_252_uses_all(self):
        closes = [80.0, 100.0, 90.0]

        pct, low = compute_lowest_close_pct(closes)

        assert low == 80.0
        # (90 - 80) / 80 * 100 = 12.5
        assert pct == 12.5

    def test_at_low(self):
        closes = [100.0, 90.0, 80.0]

        pct, low = compute_lowest_close_pct(closes)

        assert low == 80.0
        assert pct == 0.0

    def test_empty_returns_none(self):
        assert compute_lowest_close_pct([]) is None


class TestComputeStats:

    def test_returns_all_fields(self):
        # Need 1260+ for 5Y, 252+ for 1Y
        closes = [50.0] * 1300
        closes[-1] = 60.0
        timestamps = [i * 86400 for i in range(1300)]

        result = compute_stats(closes, timestamps)

        assert result is not None
        assert result["close"] == 60.0
        assert result["return5Y"] == 20.0
        assert result["return1Y"] == 20.0
        expected_keys = {"ytdPct", "highPct", "high3yr", "lowPct", "low52wk", "close", "return1Y", "return5Y"}
        assert expected_keys.issubset(result.keys())

    def test_empty_returns_none(self):
        assert compute_stats([], []) is None

    def test_single_close_returns_none(self):
        assert compute_stats([100.0], [_ts(2026, 1, 2)]) is None

    def test_no_prev_year_omits_ytd(self):
        closes = [50.0, 55.0]
        timestamps = [_ts(2026, 2, 1), _ts(2026, 2, 2)]

        result = compute_stats(closes, timestamps)

        assert result is not None
        assert "ytdPct" not in result
        assert "highPct" in result
        assert "lowPct" in result

    def test_includes_rsi(self):
        # Need > 14 closes for RSI
        closes = [100.0 + i for i in range(20)]
        timestamps = [_ts(2025, 12, 31)] + [_ts(2026, 1, i + 1) for i in range(19)]

        result = compute_stats(closes, timestamps)

        assert result is not None
        assert "rsi" in result
        assert 0 <= result["rsi"] <= 100

    def test_includes_swing_levels(self):
        # Create data with a 15% decline (triggers breakout)
        closes = [50.0, 70.0, 100.0, 85.0, 90.0]
        timestamps = [_ts(2025, 12, 31)] + [_ts(2026, 1, i + 1) for i in range(4)]

        result = compute_stats(closes, timestamps)

        assert result is not None
        assert "breakoutPrice" in result
        assert "breakoutDate" in result
        assert "breakoutPct" in result

    def test_includes_vix_returns(self):
        closes = [100.0, 90.0, 95.0, 110.0]
        timestamps = [
            _ts(2025, 12, 31), _ts(2026, 1, 2),
            _ts(2026, 1, 3), _ts(2026, 1, 6),
        ]
        vix_spikes = [{"dateString": "1/2/26", "timestamp": _ts(2026, 1, 2), "vixClose": 25.0}]

        result = compute_stats(closes, timestamps, vix_spikes=vix_spikes)

        assert result is not None
        assert "vixReturns" in result
        assert len(result["vixReturns"]) == 1

    def test_includes_forward_pe(self):
        closes = [100.0, 105.0]
        timestamps = [_ts(2025, 12, 31), _ts(2026, 1, 2)]

        result = compute_stats(closes, timestamps, forward_pe=18.5)

        assert result is not None
        assert result["forwardPE"] == 18.5

    def test_no_vix_spikes_omits_vix_returns(self):
        closes = [100.0, 105.0]
        timestamps = [_ts(2025, 12, 31), _ts(2026, 1, 2)]

        result = compute_stats(closes, timestamps)

        assert result is not None
        assert "vixReturns" not in result

    def test_includes_quarterly(self):
        closes = [100.0, 110.0, 105.0]
        timestamps = [_ts(2024, 3, 29), _ts(2024, 6, 28), _ts(2024, 7, 15)]

        result = compute_stats(closes, timestamps)

        assert result is not None
        assert "sinceQuarter" in result
        assert "duringQuarter" in result

    def test_includes_forward_pe_history(self):
        closes = [100.0, 105.0]
        timestamps = [_ts(2025, 12, 31), _ts(2026, 1, 2)]
        history = {"Q3'25": 18.5, "Q4'25": 20.0}

        result = compute_stats(closes, timestamps, forward_pe_history=history)

        assert result is not None
        assert result["forwardPEHistory"] == {"Q3'25": 18.5, "Q4'25": 20.0}

    def test_no_pe_history_omits_field(self):
        closes = [100.0, 105.0]
        timestamps = [_ts(2025, 12, 31), _ts(2026, 1, 2)]

        result = compute_stats(closes, timestamps)

        assert result is not None
        assert "forwardPEHistory" not in result


class TestSma200:

    def test_sma200d_with_sufficient_data(self):
        closes = [float(i) for i in range(1, 251)]
        timestamps = [_ts(2025, 1, 1) + i * 86400 for i in range(250)]

        result = compute_stats(closes, timestamps)

        assert result is not None
        expected_sma = round(sum(closes[-200:]) / 200, 2)
        assert result["sma200d"] == expected_sma
        assert "pctSma200d" in result

    def test_sma200d_insufficient_data(self):
        closes = [100.0 + i for i in range(50)]
        timestamps = [_ts(2025, 12, 1) + i * 86400 for i in range(50)]

        result = compute_stats(closes, timestamps)

        assert result is not None
        assert "sma200d" not in result
        assert "pctSma200d" not in result

    def test_sma200w_with_sufficient_data(self):
        closes = [100.0, 110.0]
        timestamps = [_ts(2025, 12, 31), _ts(2026, 1, 2)]
        weekly_closes = [float(i) for i in range(1, 210)]

        result = compute_stats(closes, timestamps, weekly_closes=weekly_closes)

        assert result is not None
        expected_sma = round(sum(weekly_closes[-200:]) / 200, 2)
        assert result["sma200w"] == expected_sma
        assert "pctSma200w" in result
        expected_pct = round((closes[-1] - expected_sma) / expected_sma * 100, 2)
        assert result["pctSma200w"] == expected_pct

    def test_sma200w_insufficient_data(self):
        closes = [100.0, 110.0]
        timestamps = [_ts(2025, 12, 31), _ts(2026, 1, 2)]
        weekly_closes = [float(i) for i in range(1, 100)]

        result = compute_stats(closes, timestamps, weekly_closes=weekly_closes)

        assert result is not None
        assert "sma200w" not in result
        assert "pctSma200w" not in result

    def test_sma200w_none_weekly_closes(self):
        closes = [100.0, 110.0]
        timestamps = [_ts(2025, 12, 31), _ts(2026, 1, 2)]

        result = compute_stats(closes, timestamps, weekly_closes=None)

        assert result is not None
        assert "sma200w" not in result
        assert "pctSma200w" not in result


class TestComputeReturnSince:

    def test_exact_date_match(self):
        closes = [100.0, 110.0, 120.0]
        timestamps = [
            _ts(2024, 11, 5),
            _ts(2024, 12, 1),
            _ts(2025, 1, 2),
        ]
        # (120 - 100) / 100 * 100 = 20.0
        assert compute_return_since(closes, timestamps, 2024, 11, 5) == 20.0

    def test_finds_closest_within_3_days(self):
        closes = [100.0, 110.0]
        timestamps = [_ts(2024, 11, 4), _ts(2025, 1, 2)]
        # Target is 11/5 but closest is 11/4 (1 day away)
        result = compute_return_since(closes, timestamps, 2024, 11, 5)
        assert result == 10.0

    def test_no_match_beyond_3_days_returns_none(self):
        closes = [100.0, 110.0]
        timestamps = [_ts(2024, 1, 1), _ts(2025, 1, 2)]
        assert compute_return_since(closes, timestamps, 2024, 11, 5) is None

    def test_negative_return(self):
        closes = [200.0, 180.0]
        timestamps = [_ts(2025, 1, 20), _ts(2025, 3, 1)]
        # (180 - 200) / 200 * 100 = -10.0
        assert compute_return_since(closes, timestamps, 2025, 1, 20) == -10.0

    def test_insufficient_data_returns_none(self):
        assert compute_return_since([100.0], [_ts(2025, 1, 1)], 2025, 1, 1) is None

    def test_zero_close_at_target_returns_none(self):
        closes = [0.0, 110.0]
        timestamps = [_ts(2025, 1, 20), _ts(2025, 3, 1)]
        assert compute_return_since(closes, timestamps, 2025, 1, 20) is None

    def test_rounded_to_2_decimals(self):
        closes = [100.0, 133.33]
        timestamps = [_ts(2025, 1, 20), _ts(2025, 3, 1)]
        result = compute_return_since(closes, timestamps, 2025, 1, 20)
        assert result == 33.33


class TestZeroDivisionDefensive:

    def test_highest_close_zero(self):
        # Should not crash if high is 0
        closes = [0.0, 0.0, 0.0]
        pct, high = compute_highest_close_pct(closes)
        assert high == 0.0
        assert pct == 0.0

    def test_lowest_close_zero(self):
        # Should not crash if low is 0
        closes = [0.0, 0.0, 0.0]
        _, low = compute_lowest_close_pct(closes)
        assert low == 0.0

    def test_sma200d_zero(self):
        # Should not crash if sma200d is 0
        closes = [0.0] * 250
        timestamps = [_ts(2025, 1, 1) + i * 86400 for i in range(250)]
        result = compute_stats(closes, timestamps)
        assert result["sma200d"] == 0.0
        assert result["pctSma200d"] == 0.0

    def test_sma200w_zero(self):
        # Should not crash if sma200w is 0
        closes = [100.0, 110.0]
        timestamps = [_ts(2025, 12, 31), _ts(2026, 1, 2)]
        weekly_closes = [0.0] * 210
        result = compute_stats(closes, timestamps, weekly_closes=weekly_closes)
        assert result["sma200w"] == 0.0
        assert result["pctSma200w"] == 0.0
