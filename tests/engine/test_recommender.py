from datetime import datetime

from core.models import FundamentalData, IndicatorResult, SignalType, StockQuote
from engine.recommender import Recommender


def _make_quote(symbol="TEST", price=150.0):
    return StockQuote(
        symbol=symbol, price=price, open=149.0, high=151.0,
        low=148.0, volume=1000000, timestamp=datetime.now(),
    )


def test_recommend_buy():
    config = {
        "rules": {
            "buy_conditions": [{"indicator": "rsi_14", "operator": "<", "value": 30}],
            "sell_conditions": [],
        },
        "sentiment": {"weight": 0.3},
    }
    recommender = Recommender(config)
    quote = _make_quote()
    indicators = IndicatorResult(symbol="TEST", rsi=20.0)
    signal = recommender.recommend(quote, indicators, sentiment_score=0.5)
    assert signal.signal_type == SignalType.BUY
    assert signal.confidence > 0


def test_recommend_hold_neutral():
    config = {
        "rules": {
            "buy_conditions": [{"indicator": "rsi_14", "operator": "<", "value": 30}],
            "sell_conditions": [{"indicator": "rsi_14", "operator": ">", "value": 70}],
        },
        "sentiment": {"weight": 0.3},
    }
    recommender = Recommender(config)
    quote = _make_quote()
    indicators = IndicatorResult(symbol="TEST", rsi=50.0)
    signal = recommender.recommend(quote, indicators, sentiment_score=0.0)
    assert signal.signal_type == SignalType.HOLD


def test_recommend_with_fundamentals():
    config = {
        "rules": {
            "buy_conditions": [
                {"indicator": "rsi_14", "operator": "<", "value": 30},
                {"indicator": "per", "operator": "<", "value": 15},
            ],
            "sell_conditions": [],
        },
        "sentiment": {"weight": 0.0},
    }
    recommender = Recommender(config)
    quote = _make_quote()
    indicators = IndicatorResult(symbol="TEST", rsi=25.0)
    fundamentals = FundamentalData(symbol="TEST", per=10.0)
    signal = recommender.recommend(quote, indicators, fundamentals)
    assert signal.signal_type == SignalType.BUY
    assert signal.fundamentals is not None
