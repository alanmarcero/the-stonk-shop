from datetime import datetime, timezone

from src.worker.vix import detect_spikes, compute_spike_returns


def _ts(year, month, day):
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp())


class TestDetectSpikes:

    def test_empty_returns_empty(self):
        assert detect_spikes([], []) == []

    def test_mismatched_lengths_returns_empty(self):
        assert detect_spikes([25.0, 30.0], [1000]) == []

    def test_no_spikes_below_threshold(self):
        closes = [15.0, 16.0, 18.0, 19.0]
        timestamps = [_ts(2024, 1, i + 1) for i in range(4)]

        assert detect_spikes(closes, timestamps) == []

    def test_single_spike(self):
        closes = [15.0, 25.0, 15.0]
        timestamps = [_ts(2024, 1, 1), _ts(2024, 1, 2), _ts(2024, 1, 3)]

        result = detect_spikes(closes, timestamps)

        assert len(result) == 1
        assert result[0]["vixClose"] == 25.0
        assert result[0]["timestamp"] == _ts(2024, 1, 2)
        assert "dateString" in result[0]

    def test_cluster_takes_peak(self):
        # Three consecutive spike days — should cluster into one, take peak
        closes = [15.0, 22.0, 28.0, 25.0, 15.0]
        timestamps = [_ts(2024, 1, i + 1) for i in range(5)]

        result = detect_spikes(closes, timestamps)

        assert len(result) == 1
        assert result[0]["vixClose"] == 28.0

    def test_two_separate_clusters(self):
        # Two spikes with large gap (> gapDays + 1)
        closes = [15.0, 25.0, 15.0, 15.0, 15.0, 15.0, 15.0, 15.0, 15.0, 30.0, 15.0]
        timestamps = [_ts(2024, 1, i + 1) for i in range(11)]

        result = detect_spikes(closes, timestamps)

        assert len(result) == 2
        assert result[0]["vixClose"] == 25.0
        assert result[1]["vixClose"] == 30.0

    def test_custom_threshold(self):
        closes = [10.0, 15.0, 10.0]
        timestamps = [_ts(2024, 1, 1), _ts(2024, 1, 2), _ts(2024, 1, 3)]

        result_default = detect_spikes(closes, timestamps)
        result_custom = detect_spikes(closes, timestamps, threshold=15.0)

        assert len(result_default) == 0
        assert len(result_custom) == 1

    def test_gap_boundary(self):
        # Gap of exactly gapDays + 1 between indices => same cluster
        closes = [15.0] * 10
        closes[0] = 25.0  # index 0
        closes[6] = 30.0  # index 6, gap = 6 = gapDays(5) + 1 => same cluster
        timestamps = [_ts(2024, 1, i + 1) for i in range(10)]

        result = detect_spikes(closes, timestamps)

        assert len(result) == 1
        assert result[0]["vixClose"] == 30.0

    def test_gap_exceeds_boundary(self):
        # Gap of gapDays + 2 => separate clusters
        closes = [15.0] * 10
        closes[0] = 25.0  # index 0
        closes[7] = 30.0  # index 7, gap = 7 > gapDays(5) + 1 => separate
        timestamps = [_ts(2024, 1, i + 1) for i in range(10)]

        result = detect_spikes(closes, timestamps)

        assert len(result) == 2

    def test_date_format(self):
        closes = [25.0]
        timestamps = [_ts(2024, 3, 5)]

        result = detect_spikes(closes, timestamps)

        assert result[0]["dateString"] == "3/5/24"

    def test_vix_close_rounded(self):
        closes = [25.123456]
        timestamps = [_ts(2024, 1, 1)]

        result = detect_spikes(closes, timestamps)

        assert result[0]["vixClose"] == 25.12


class TestComputeSpikeReturns:

    def test_empty_spikes(self):
        assert compute_spike_returns([], [100.0], [1000], 110.0) == []

    def test_empty_closes(self):
        spikes = [{"dateString": "1/1/24", "timestamp": 1000, "vixClose": 25.0}]
        assert compute_spike_returns(spikes, [], [], 110.0) == []

    def test_basic_return_calculation(self):
        spikes = [{"dateString": "1/2/24", "timestamp": _ts(2024, 1, 2), "vixClose": 25.0}]
        closes = [100.0, 90.0, 95.0, 110.0]
        timestamps = [_ts(2024, 1, 1), _ts(2024, 1, 2), _ts(2024, 1, 3), _ts(2024, 1, 4)]

        result = compute_spike_returns(spikes, closes, timestamps, 110.0)

        assert len(result) == 1
        assert result[0]["spikeClose"] == 90.0
        # (110 - 90) / 90 * 100 = 22.22
        assert result[0]["pctGain"] == 22.22
        assert result[0]["dateString"] == "1/2/24"
        assert result[0]["vixClose"] == 25.0

    def test_negative_return(self):
        spikes = [{"dateString": "1/2/24", "timestamp": _ts(2024, 1, 2), "vixClose": 25.0}]
        closes = [100.0, 110.0, 105.0]
        timestamps = [_ts(2024, 1, 1), _ts(2024, 1, 2), _ts(2024, 1, 3)]

        result = compute_spike_returns(spikes, closes, timestamps, 100.0)

        assert len(result) == 1
        assert result[0]["pctGain"] < 0

    def test_finds_closest_timestamp(self):
        # Spike timestamp doesn't match exactly but is within 3 days
        spike_ts = _ts(2024, 1, 3)  # Wednesday
        spikes = [{"dateString": "1/3/24", "timestamp": spike_ts, "vixClose": 25.0}]
        closes = [100.0, 95.0]
        timestamps = [_ts(2024, 1, 2), _ts(2024, 1, 4)]  # Tue, Thu

        result = compute_spike_returns(spikes, closes, timestamps, 100.0)

        assert len(result) == 1

    def test_skips_spike_with_no_matching_close(self):
        spike_ts = _ts(2024, 6, 1)
        spikes = [{"dateString": "6/1/24", "timestamp": spike_ts, "vixClose": 25.0}]
        closes = [100.0]
        timestamps = [_ts(2024, 1, 1)]  # Way too far

        result = compute_spike_returns(spikes, closes, timestamps, 110.0)

        assert len(result) == 0

    def test_multiple_spikes(self):
        spikes = [
            {"dateString": "1/2/24", "timestamp": _ts(2024, 1, 2), "vixClose": 25.0},
            {"dateString": "3/1/24", "timestamp": _ts(2024, 3, 1), "vixClose": 30.0},
        ]
        closes = [100.0, 90.0, 95.0, 80.0, 85.0]
        timestamps = [
            _ts(2024, 1, 1), _ts(2024, 1, 2),
            _ts(2024, 2, 1),
            _ts(2024, 3, 1), _ts(2024, 3, 15),
        ]

        result = compute_spike_returns(spikes, closes, timestamps, 110.0)

        assert len(result) == 2

    def test_skips_zero_spike_close(self):
        spikes = [{"dateString": "1/1/24", "timestamp": _ts(2024, 1, 1), "vixClose": 25.0}]
        closes = [0.0]
        timestamps = [_ts(2024, 1, 1)]

        result = compute_spike_returns(spikes, closes, timestamps, 100.0)

        assert len(result) == 0
