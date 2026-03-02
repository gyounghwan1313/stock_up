import pandas as pd
import pytest

from indicators.technical import (
    calculate_indicators,
    compute_bollinger,
    compute_macd,
    compute_rsi,
    compute_sma,
)


@pytest.fixture
def sample_prices():
    return pd.Series([
        44, 44.34, 44.09, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84,
        46.08, 45.89, 46.03, 45.61, 46.28, 46.28, 46.00, 46.03, 46.41,
        46.22, 45.64, 46.21, 46.25, 45.71, 46.45, 45.78, 45.35, 44.03,
        44.18, 44.22, 44.57, 43.42, 42.66,
    ])


def test_compute_rsi_returns_series(sample_prices):
    rsi = compute_rsi(sample_prices, period=14)
    assert len(rsi) == len(sample_prices)
    last_rsi = rsi.iloc[-1]
    assert 0 <= last_rsi <= 100


def test_compute_rsi_oversold():
    # 계속 하락하는 가격 → RSI 낮음
    declining = pd.Series([100 - i * 2 for i in range(30)])
    rsi = compute_rsi(declining, period=14)
    assert rsi.iloc[-1] < 30


def test_compute_macd(sample_prices):
    macd_line, signal_line, histogram = compute_macd(sample_prices)
    assert len(macd_line) == len(sample_prices)
    assert len(signal_line) == len(sample_prices)
    assert len(histogram) == len(sample_prices)


def test_compute_sma(sample_prices):
    sma = compute_sma(sample_prices, period=5)
    assert pd.notna(sma.iloc[-1])
    assert pd.isna(sma.iloc[0])  # 첫 4개는 NaN


def test_compute_bollinger(sample_prices):
    upper, middle, lower = compute_bollinger(sample_prices, period=20, num_std=2.0)
    last_idx = -1
    assert upper.iloc[last_idx] > middle.iloc[last_idx] > lower.iloc[last_idx]


def test_calculate_indicators():
    df = pd.DataFrame({
        "Close": [100 + i * 0.5 for i in range(60)],
        "High": [101 + i * 0.5 for i in range(60)],
        "Low": [99 + i * 0.5 for i in range(60)],
        "Volume": [1000000] * 60,
    })
    config = {
        "indicators": {
            "rsi_period": 14,
            "macd_fast": 12,
            "macd_slow": 26,
            "macd_signal": 9,
            "sma_periods": [20, 50],
            "bollinger_period": 20,
            "bollinger_std": 2,
        }
    }
    result = calculate_indicators(df, "TEST", config)
    assert result.symbol == "TEST"
    assert result.rsi is not None
    assert result.macd is not None
    assert 20 in result.sma
    assert 50 in result.sma
