import logging

from core.models import IndicatorResult, SignalType

logger = logging.getLogger(__name__)


def evaluate_rules(indicators: IndicatorResult, rules: dict) -> tuple[SignalType, list[str]]:
    buy_reasons: list[str] = []
    sell_reasons: list[str] = []

    for cond in rules.get("buy_conditions", []):
        indicator_name = cond["indicator"]
        value = _get_indicator_value(indicators, indicator_name)
        if value is not None and _evaluate(value, cond["operator"], cond["value"]):
            buy_reasons.append(f"{indicator_name}={value:.2f} {cond['operator']} {cond['value']}")

    for cond in rules.get("sell_conditions", []):
        indicator_name = cond["indicator"]
        value = _get_indicator_value(indicators, indicator_name)
        if value is not None and _evaluate(value, cond["operator"], cond["value"]):
            sell_reasons.append(f"{indicator_name}={value:.2f} {cond['operator']} {cond['value']}")

    if buy_reasons and not sell_reasons:
        return SignalType.BUY, buy_reasons
    elif sell_reasons and not buy_reasons:
        return SignalType.SELL, sell_reasons
    elif buy_reasons and sell_reasons:
        if len(buy_reasons) >= len(sell_reasons):
            return SignalType.BUY, buy_reasons + [f"(conflicting: {r})" for r in sell_reasons]
        return SignalType.SELL, sell_reasons + [f"(conflicting: {r})" for r in buy_reasons]
    return SignalType.HOLD, ["No conditions triggered"]


def _get_indicator_value(indicators: IndicatorResult, name: str) -> float | None:
    if name.startswith("rsi"):
        return indicators.rsi
    elif name == "macd":
        return indicators.macd
    elif name == "macd_histogram":
        return indicators.macd_histogram
    elif name.startswith("sma_"):
        period = int(name.split("_")[1])
        return indicators.sma.get(period)
    return None


def _evaluate(actual: float, operator: str, threshold: float) -> bool:
    if operator == "<":
        return actual < threshold
    elif operator == ">":
        return actual > threshold
    elif operator == "<=":
        return actual <= threshold
    elif operator == ">=":
        return actual >= threshold
    return False
