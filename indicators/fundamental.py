from core.models import FundamentalData


def evaluate_fundamentals(data: FundamentalData, rules: dict) -> list[str]:
    reasons: list[str] = []
    buy_conditions = rules.get("buy_conditions", [])
    sell_conditions = rules.get("sell_conditions", [])

    for cond in buy_conditions:
        indicator = cond.get("indicator", "")
        if indicator == "per" and data.per is not None:
            if _evaluate(data.per, cond["operator"], cond["value"]):
                reasons.append(f"BUY: PER {data.per:.1f} {cond['operator']} {cond['value']}")
        elif indicator == "pbr" and data.pbr is not None:
            if _evaluate(data.pbr, cond["operator"], cond["value"]):
                reasons.append(f"BUY: PBR {data.pbr:.1f} {cond['operator']} {cond['value']}")

    for cond in sell_conditions:
        indicator = cond.get("indicator", "")
        if indicator == "per" and data.per is not None:
            if _evaluate(data.per, cond["operator"], cond["value"]):
                reasons.append(f"SELL: PER {data.per:.1f} {cond['operator']} {cond['value']}")

    return reasons


def _evaluate(actual: float, operator: str, threshold: float) -> bool:
    if operator == "<":
        return actual < threshold
    elif operator == ">":
        return actual > threshold
    elif operator == "<=":
        return actual <= threshold
    elif operator == ">=":
        return actual >= threshold
    elif operator == "==":
        return actual == threshold
    return False
