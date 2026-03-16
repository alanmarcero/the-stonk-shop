from src.worker.rsi import calculate, DEFAULT_PERIOD


class TestCalculate:

    def test_insufficient_data_returns_none(self):
        closes = [100.0] * 14  # Need > 14
        assert calculate(closes) is None

    def test_exactly_period_returns_none(self):
        closes = [100.0] * DEFAULT_PERIOD
        assert calculate(closes) is None

    def test_one_more_than_period_returns_value(self):
        closes = list(range(1, DEFAULT_PERIOD + 2))  # 15 values
        result = calculate(closes)
        assert result is not None
        assert 0 <= result <= 100

    def test_all_gains_returns_100(self):
        # Strictly increasing — all gains, no losses
        closes = [float(i) for i in range(100)]
        result = calculate(closes)
        assert result == 100.0

    def test_all_losses(self):
        # Strictly decreasing — all losses, no gains
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

    def test_known_rsi_value(self):
        # Alternating up/down pattern
        closes = []
        price = 100.0
        for i in range(50):
            if i % 2 == 0:
                price += 2.0
            else:
                price -= 1.0
            closes.append(price)

        result = calculate(closes)
        assert result is not None
        # With 2x gains vs 1x losses, RSI should be elevated
        assert result > 60

    def test_bearish_rsi(self):
        # Alternating: small up, big down
        closes = []
        price = 200.0
        for i in range(50):
            if i % 2 == 0:
                price -= 3.0
            else:
                price += 1.0
            closes.append(price)

        result = calculate(closes)
        assert result is not None
        assert result < 40

    def test_custom_period(self):
        closes = list(range(1, 30))
        result = calculate(closes, period=5)
        assert result is not None
        assert 0 <= result <= 100

    def test_returns_rounded_to_2_decimals(self):
        closes = []
        price = 100.0
        for i in range(50):
            price += (-1) ** i * 1.37
            closes.append(price)

        result = calculate(closes)
        assert result is not None
        assert result == round(result, 2)

    def test_empty_returns_none(self):
        assert calculate([]) is None

    def test_single_close_returns_none(self):
        assert calculate([100.0]) is None


class TestDefaultPeriod:

    def test_default_period_is_14(self):
        assert DEFAULT_PERIOD == 14
