from core.models import NewsAlert, Signal, SignalType


SIGNAL_EMOJI = {
    SignalType.BUY: ":chart_with_upwards_trend:",
    SignalType.SELL: ":chart_with_downwards_trend:",
    SignalType.HOLD: ":pause_button:",
}

SIGNAL_COLOR = {
    SignalType.BUY: "#36a64f",
    SignalType.SELL: "#d00000",
    SignalType.HOLD: "#cccccc",
}

SIGNAL_KO = {
    SignalType.BUY: "매수",
    SignalType.SELL: "매도",
    SignalType.HOLD: "관망",
}

_OP_KO = {"<": "미만", ">": "초과", "<=": "이하", ">=": "이상"}


def _rsi_context(rsi: float) -> str:
    if rsi < 30:
        return "과매도 구간"
    if rsi > 70:
        return "과매수 구간"
    return "중립"


def _macd_context(macd: float) -> str:
    return "상승 모멘텀" if macd > 0 else "하락 모멘텀"


def _sentiment_context(score: float) -> str:
    if score >= 0.3:
        return "긍정적"
    if score <= -0.3:
        return "부정적"
    return "중립"


def _translate_reason(reason: str) -> str:
    """엔진에서 생성된 영문 reason 문자열을 한글로 변환"""
    if reason == "No conditions triggered":
        return "기술 지표 조건 없음"

    # "Sentiment: -0.40"
    if reason.startswith("Sentiment: "):
        try:
            score = float(reason.split(": ", 1)[1])
            return f"감성 점수 {score:+.2f} ({_sentiment_context(score)})"
        except ValueError:
            return reason

    # "(conflicting: ...)"
    if reason.startswith("(conflicting: ") and reason.endswith(")"):
        inner = reason[len("(conflicting: "):-1]
        return f"(상충 신호: {_translate_reason(inner)})"

    # "BUY: PER 369.3 > 40" or "SELL: PBR 1.2 < 3"
    for eng_prefix, ko_prefix in (("BUY: ", "매수 조건 — "), ("SELL: ", "매도 조건 — ")):
        if reason.startswith(eng_prefix):
            parts = reason[len(eng_prefix):].split()  # ["PER", "369.3", ">", "40"]
            if len(parts) == 4:
                indicator, value, op, threshold = parts
                op_ko = _OP_KO.get(op, op)
                return f"{ko_prefix}{indicator} {value} (기준 {threshold} {op_ko})"
            return f"{ko_prefix}{reason[len(eng_prefix):]}"

    # "rsi=41.20 < 30" or "macd=-6.99 > 0" (tech indicator conditions)
    for op in ("<=", ">=", "<", ">"):
        if f" {op} " in reason:
            left, threshold = reason.split(f" {op} ", 1)
            if "=" in left:
                name, value = left.split("=", 1)
                op_ko = _OP_KO.get(op, op)
                return f"{name.upper()} {float(value):.2f}가 기준치 {threshold} {op_ko}"
            break

    return reason


def format_signal_message(signal: Signal) -> str:
    emoji = SIGNAL_EMOJI[signal.signal_type]
    signal_ko = SIGNAL_KO[signal.signal_type]
    lines = [
        f"{emoji} *{signal_ko}* | *{signal.symbol}* @ ${signal.price:.2f}",
        f"신뢰도: {signal.confidence:.0%}",
    ]

    if signal.indicators:
        ind = signal.indicators
        parts = []
        if ind.rsi is not None:
            parts.append(f"RSI={ind.rsi:.1f} ({_rsi_context(ind.rsi)})")
        if ind.macd is not None:
            parts.append(f"MACD={ind.macd:.2f} ({_macd_context(ind.macd)})")
        if parts:
            lines.append("기술 지표: " + " | ".join(parts))

    if signal.fundamentals:
        f = signal.fundamentals
        parts = []
        if f.per is not None:
            parts.append(f"PER={f.per:.1f}")
        if f.pbr is not None:
            parts.append(f"PBR={f.pbr:.1f}")
        if parts:
            lines.append("기본 지표: " + " | ".join(parts))

    if signal.sentiment_score:
        ctx = _sentiment_context(signal.sentiment_score)
        lines.append(f"감성 점수: {signal.sentiment_score:+.2f} ({ctx})")

    lines.append("판단 근거:")
    for r in signal.reasons:
        lines.append(f"  • {_translate_reason(r)}")

    return "\n".join(lines)


def format_signal_attachment(signal: Signal) -> dict:
    signal_ko = SIGNAL_KO[signal.signal_type]
    translated_reasons = [_translate_reason(r) for r in signal.reasons]
    return {
        "color": SIGNAL_COLOR[signal.signal_type],
        "title": f"{signal_ko} {signal.symbol}",
        "fields": [
            {"title": "현재가", "value": f"${signal.price:.2f}", "short": True},
            {"title": "신뢰도", "value": f"{signal.confidence:.0%}", "short": True},
            {"title": "판단 근거", "value": " / ".join(translated_reasons), "short": False},
        ],
    }


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

    # 관련 뉴스
    lines.append("관련 뉴스:")
    for item in alert.news_items[:5]:
        sentiment_label = "긍정" if item.sentiment_score >= 0 else "부정"
        lines.append(f"  • \"{item.title}\" ({item.sentiment_score:+.2f} {sentiment_label})")
    lines.append("")

    # 종합 평가
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


def format_discovery_message(discovered: list) -> str:
    if not discovered:
        return ""
    lines = [":mag: *발굴 종목*"]
    for s in discovered[:10]:
        lines.append(f"  • *{s.symbol}* ({s.name}) — {s.discovery_reason}")
    return "\n".join(lines)
