from unittest.mock import MagicMock, patch

from screener.stock_screener import StockScreener


def test_screener_init():
    config = {
        "universes": ["sp500"],
        "max_candidates": 10,
        "filters": {"min_market_cap": 1_000_000_000},
    }
    screener = StockScreener(config)
    assert screener.max_candidates == 10
    assert screener.universes == ["sp500"]


def _mock_sp500():
    return [
        {"symbol": "AAPL", "name": "Apple", "sector": "Tech"},
        {"symbol": "TINY", "name": "Tiny Corp", "sector": "Tech"},
    ]


@patch("screener.stock_screener.UNIVERSE_FETCHERS", {"sp500": _mock_sp500})
@patch("screener.stock_screener.yf.Tickers")
def test_discover_stocks_filters(mock_tickers):
    mock_ticker_aapl = MagicMock()
    mock_ticker_aapl.info = {
        "marketCap": 3_000_000_000_000,
        "averageDailyVolume10Day": 50_000_000,
        "trailingPE": 28,
        "currentPrice": 190.0,
        "52WeekChange": 0.15,
        "sector": "Technology",
    }

    mock_ticker_tiny = MagicMock()
    mock_ticker_tiny.info = {
        "marketCap": 500_000,
        "averageDailyVolume10Day": 1000,
        "trailingPE": 5,
        "currentPrice": 1.0,
    }

    mock_tickers.return_value.tickers = {"AAPL": mock_ticker_aapl, "TINY": mock_ticker_tiny}

    config = {
        "universes": ["sp500"],
        "max_candidates": 10,
        "filters": {"min_market_cap": 1_000_000_000, "min_avg_volume": 500_000},
    }
    screener = StockScreener(config)
    results = screener.discover_stocks()

    symbols = [r.symbol for r in results]
    assert "AAPL" in symbols
    assert "TINY" not in symbols
