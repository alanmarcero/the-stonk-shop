from src.worker.rsi import calculate


class TestCalculate:

    def test_insufficient_data_returns_none(self):
        closes = [100.0] * 14  # Need > 14
        assert calculate(closes) is None

    def test_exactly_14_closes_returns_none(self):
        closes = [100.0] * 14
        assert calculate(closes) is None

    def test_15_closes_returns_value(self):
        closes = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
        result = calculate(closes)
        assert result is not None
        assert 0 <= result <= 100

    def test_all_gains_returns_100(self):
        closes = [float(i) for i in range(100)]
        result = calculate(closes)
        assert result == 100.0

    def test_all_losses(self):
        closes = [float(100 - i) for i in range(100)]
        result = calculate(closes)
        assert result is not None
        assert result < 5  # Very low RSI

    def test_constant_price(self):
        closes = [100.0] * 20
        result = calculate(closes)
        # No gains, no losses => avgGain=0, avgLoss=0 => special case
        # When avgLoss == 0, should return 100.0
        assert result == 100.0

    def test_alternating_2x_gains_vs_1x_losses_gives_elevated_rsi(self):
        # Alternating +2, -1 pattern over 50 bars (gains outweigh losses)
        closes = [
            102.0, 101.0, 103.0, 102.0, 104.0, 103.0, 105.0, 104.0, 106.0, 105.0,
            107.0, 106.0, 108.0, 107.0, 109.0, 108.0, 110.0, 109.0, 111.0, 110.0,
            112.0, 111.0, 113.0, 112.0, 114.0, 113.0, 115.0, 114.0, 116.0, 115.0,
            117.0, 116.0, 118.0, 117.0, 119.0, 118.0, 120.0, 119.0, 121.0, 120.0,
            122.0, 121.0, 123.0, 122.0, 124.0, 123.0, 125.0, 124.0, 126.0, 125.0,
        ]
        result = calculate(closes)
        assert result is not None
        assert result > 60

    def test_alternating_3x_losses_vs_1x_gains_gives_low_rsi(self):
        # Alternating -3, +1 pattern over 50 bars (losses outweigh gains)
        closes = [
            197.0, 198.0, 195.0, 196.0, 193.0, 194.0, 191.0, 192.0, 189.0, 190.0,
            187.0, 188.0, 185.0, 186.0, 183.0, 184.0, 181.0, 182.0, 179.0, 180.0,
            177.0, 178.0, 175.0, 176.0, 173.0, 174.0, 171.0, 172.0, 169.0, 170.0,
            167.0, 168.0, 165.0, 166.0, 163.0, 164.0, 161.0, 162.0, 159.0, 160.0,
            157.0, 158.0, 155.0, 156.0, 153.0, 154.0, 151.0, 152.0, 149.0, 150.0,
        ]
        result = calculate(closes)
        assert result is not None
        assert result < 40

    def test_custom_period(self):
        closes = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
                  11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
                  21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0]
        result = calculate(closes, period=5)
        assert result is not None
        assert 0 <= result <= 100

    def test_returns_rounded_to_2_decimals(self):
        # Explicit alternating +1.37 / -1.37 pattern
        closes = [
            100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37,
            100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37,
            100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37,
            100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37,
            100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37, 100.0, 101.37,
        ]
        result = calculate(closes)
        assert result is not None
        assert result == round(result, 2)

    def test_empty_returns_none(self):
        assert calculate([]) is None

    def test_single_close_returns_none(self):
        assert calculate([100.0]) is None
