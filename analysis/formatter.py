from shared.models import MacroData, Signal, SignalType


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
    if reason == "No conditions triggered":
        return "기술 지표 조건 없음"

    if reason.startswith("Sentiment: "):
        try:
            score = float(reason.split(": ", 1)[1])
            return f"감성 점수 {score:+.2f} ({_sentiment_context(score)})"
        except ValueError:
            return reason

    if reason.startswith("(conflicting: ") and reason.endswith(")"):
        inner = reason[len("(conflicting: "):-1]
        return f"(상충 신호: {_translate_reason(inner)})"

    for eng_prefix, ko_prefix in (("BUY: ", "매수 조건 — "), ("SELL: ", "매도 조건 — ")):
        if reason.startswith(eng_prefix):
            parts = reason[len(eng_prefix):].split()
            if len(parts) == 4:
                indicator, value, op, threshold = parts
                op_ko = _OP_KO.get(op, op)
                return f"{ko_prefix}{indicator} {value} (기준 {threshold} {op_ko})"
            return f"{ko_prefix}{reason[len(eng_prefix):]}"

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
        if f.forward_pe is not None:
            parts.append(f"Fwd PE={f.forward_pe:.1f}")
        if f.pbr is not None:
            parts.append(f"PBR={f.pbr:.1f}")
        if f.peg_ratio is not None:
            parts.append(f"PEG={f.peg_ratio:.2f}")
        if parts:
            lines.append("기본 지표: " + " | ".join(parts))

        val_parts = []
        if f.ev_to_ebitda is not None:
            val_parts.append(f"EV/EBITDA={f.ev_to_ebitda:.1f}")
        if f.ev_to_revenue is not None:
            val_parts.append(f"EV/Sales={f.ev_to_revenue:.1f}")
        if f.roe is not None:
            val_parts.append(f"ROE={f.roe:.1%}")
        if f.roic is not None:
            val_parts.append(f"ROIC={f.roic:.1%}")
        if val_parts:
            lines.append("밸류에이션: " + " | ".join(val_parts))

        margin_parts = []
        if f.operating_margin is not None:
            margin_parts.append(f"영업={f.operating_margin:.1%}")
        if f.net_margin is not None:
            margin_parts.append(f"순이익={f.net_margin:.1%}")
        if f.fcf_margin is not None:
            margin_parts.append(f"FCF={f.fcf_margin:.1%}")
        if margin_parts:
            lines.append("마진: " + " | ".join(margin_parts))

        if f.target_mean_price is not None:
            upside = ((f.target_mean_price / signal.price) - 1) * 100 if signal.price > 0 else 0
            lines.append(f"애널리스트 목표가: ${f.target_mean_price:.0f} ({upside:+.1f}%) | 추천: {f.recommendation or 'N/A'}")

        if f.short_pct_of_float is not None:
            lines.append(f"공매도 비율: {f.short_pct_of_float:.1%}")

    if signal.indicators:
        ind = signal.indicators
        ext_parts = []
        if ind.golden_cross:
            ext_parts.append(":star: 골든크로스 발생!")
        if ind.death_cross:
            ext_parts.append(":skull: 데스크로스 발생!")
        if ind.support is not None and ind.resistance is not None:
            ext_parts.append(f"지지=${ind.support:.2f} | 저항=${ind.resistance:.2f}")
        if ext_parts:
            lines.append("기술 확장: " + " | ".join(ext_parts))

    if signal.sentiment_score:
        ctx = _sentiment_context(signal.sentiment_score)
        lines.append(f"감성 점수: {signal.sentiment_score:+.2f} ({ctx})")

    lines.append("판단 근거:")
    for r in signal.reasons:
        lines.append(f"  • {_translate_reason(r)}")

    return "\n".join(lines)


def _format_pct(val: float | None) -> str:
    if val is None:
        return "N/A"
    return f"{val:.1%}"


def _format_num(val: float | None, fmt: str = ",.0f") -> str:
    if val is None:
        return "N/A"
    return f"{val:{fmt}}"


def format_macro_message(macro: MacroData) -> str:
    regime = "강세 (Above SMA200)" if macro.sp500_above_sma200 else "약세 (Below SMA200)"
    if macro.sp500_above_sma200 is None:
        regime = "N/A"

    spread_str = f"{macro.yield_spread_10y_2y:+.2f}%" if macro.yield_spread_10y_2y is not None else "N/A"
    inverted = " :warning: 역전" if macro.yield_spread_10y_2y is not None and macro.yield_spread_10y_2y < 0 else ""

    vix_emoji = ""
    if macro.vix is not None:
        if macro.vix > 30:
            vix_emoji = " :rotating_light: 공포"
        elif macro.vix > 20:
            vix_emoji = " :warning: 주의"
        else:
            vix_emoji = " :white_check_mark: 안정"

    lines = [
        ":globe_with_meridians: *매크로 시장 환경*",
        f"S&P500: {_format_num(macro.sp500, ',.1f')} | 시장 레짐: {regime}",
        f"VIX: {_format_num(macro.vix, '.1f')}{vix_emoji}",
        f"10Y 금리: {_format_num(macro.treasury_10y, '.2f')}% | 2Y 금리: {_format_num(macro.treasury_2y, '.2f')}%",
        f"10Y-2Y 스프레드: {spread_str}{inverted}",
        f"DXY (달러): {_format_num(macro.dxy, '.2f')} | WTI: ${_format_num(macro.wti_oil, '.2f')} | 금: ${_format_num(macro.gold, '.2f')}",
    ]
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
