from core.models import FundamentalData
from indicators.fundamental import evaluate_fundamentals


def test_buy_condition_per():
    data = FundamentalData(symbol="TEST", per=10.0)
    rules = {"buy_conditions": [{"indicator": "per", "operator": "<", "value": 15}]}
    reasons = evaluate_fundamentals(data, rules)
    assert len(reasons) == 1
    assert "BUY" in reasons[0]


def test_sell_condition_per():
    data = FundamentalData(symbol="TEST", per=50.0)
    rules = {"sell_conditions": [{"indicator": "per", "operator": ">", "value": 40}]}
    reasons = evaluate_fundamentals(data, rules)
    assert len(reasons) == 1
    assert "SELL" in reasons[0]


def test_no_conditions_triggered():
    data = FundamentalData(symbol="TEST", per=20.0)
    rules = {
        "buy_conditions": [{"indicator": "per", "operator": "<", "value": 15}],
        "sell_conditions": [{"indicator": "per", "operator": ">", "value": 40}],
    }
    reasons = evaluate_fundamentals(data, rules)
    assert len(reasons) == 0


def test_none_values_skipped():
    data = FundamentalData(symbol="TEST", per=None, pbr=None)
    rules = {"buy_conditions": [{"indicator": "per", "operator": "<", "value": 15}]}
    reasons = evaluate_fundamentals(data, rules)
    assert len(reasons) == 0
