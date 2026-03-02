from core.models import IndicatorResult, SignalType
from engine.rule_engine import evaluate_rules


def test_buy_signal_rsi_low():
    indicators = IndicatorResult(symbol="TEST", rsi=25.0)
    rules = {
        "buy_conditions": [{"indicator": "rsi_14", "operator": "<", "value": 30}],
        "sell_conditions": [],
    }
    signal_type, reasons = evaluate_rules(indicators, rules)
    assert signal_type == SignalType.BUY
    assert len(reasons) == 1


def test_sell_signal_rsi_high():
    indicators = IndicatorResult(symbol="TEST", rsi=75.0)
    rules = {
        "buy_conditions": [],
        "sell_conditions": [{"indicator": "rsi_14", "operator": ">", "value": 70}],
    }
    signal_type, reasons = evaluate_rules(indicators, rules)
    assert signal_type == SignalType.SELL


def test_hold_when_no_conditions():
    indicators = IndicatorResult(symbol="TEST", rsi=50.0)
    rules = {
        "buy_conditions": [{"indicator": "rsi_14", "operator": "<", "value": 30}],
        "sell_conditions": [{"indicator": "rsi_14", "operator": ">", "value": 70}],
    }
    signal_type, reasons = evaluate_rules(indicators, rules)
    assert signal_type == SignalType.HOLD


def test_conflicting_signals():
    indicators = IndicatorResult(symbol="TEST", rsi=25.0, macd=5.0)
    rules = {
        "buy_conditions": [{"indicator": "rsi_14", "operator": "<", "value": 30}],
        "sell_conditions": [{"indicator": "macd", "operator": ">", "value": 3}],
    }
    signal_type, reasons = evaluate_rules(indicators, rules)
    # 동일 수 → buy_count >= sell_count이면 BUY
    assert signal_type in (SignalType.BUY, SignalType.SELL)
    assert any("conflicting" in r for r in reasons)
