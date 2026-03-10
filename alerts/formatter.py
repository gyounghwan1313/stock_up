from shared.models import NewsAlert


_INDICATOR_KO = {
    "rsi": "RSI",
    "per": "PER",
    "pbr": "PBR",
    "bollinger": "볼린저",
}

_NORM_CONTEXT = {
    "rsi": lambda n: "과매도" if n < -0.3 else ("과매수" if n > 0.3 else "중립"),
    "per": lambda n: "저평가" if n < -0.3 else ("고평가" if n > 0.3 else "적정"),
    "pbr": lambda n: "저평가" if n < -0.3 else ("고평가" if n > 0.3 else "적정"),
    "bollinger": lambda n: "하단 이탈" if n < -0.5 else ("상단 이탈" if n > 0.5 else "밴드 내"),
}


def _format_raw_value(key: str, raw) -> str:
    if key == "bollinger":
        return ""
    if raw is None:
        return "N/A"
    return f"{raw:.1f}"


def format_news_alert_message(alert: NewsAlert) -> str:
    lines = [f":newspaper: *뉴스 기반 분석* | *{alert.symbol}* @ ${alert.price:.2f}"]
    lines.append("")

    lines.append("관련 뉴스:")
    for item in alert.news_items[:5]:
        sentiment_label = "긍정" if item.sentiment_score >= 0 else "부정"
        lines.append(f"  • \"{item.title}\" ({item.sentiment_score:+.2f} {sentiment_label})")
    lines.append("")

    lines.append(f"종합 평가: {alert.valuation} (composite={alert.composite_score:+.2f})")
    indicator_keys = ["rsi", "per", "pbr", "bollinger"]
    for i, key in enumerate(indicator_keys):
        detail = alert.indicator_scores.get(key)
        if detail is None or detail.get("normalized") is None:
            continue
        raw = detail["raw"]
        norm = detail["normalized"]
        weight = detail["weight"]
        name = _INDICATOR_KO.get(key, key)
        context_fn = _NORM_CONTEXT.get(key)
        context = context_fn(norm) if context_fn else ""

        is_last = i == len(indicator_keys) - 1 or all(
            alert.indicator_scores.get(k, {}).get("normalized") is None
            for k in indicator_keys[i + 1:]
        )
        prefix = "└" if is_last else "├"

        if key == "bollinger":
            lines.append(f"  {prefix} {name}: {context} → {norm:+.2f} [가중치 {weight:.0%}]")
        else:
            raw_str = _format_raw_value(key, raw)
            lines.append(f"  {prefix} {name}={raw_str} → {norm:+.2f} ({context}) [가중치 {weight:.0%}]")

    lines.append("")
    lines.append(f"결론: {alert.conclusion}")

    return "\n".join(lines)
