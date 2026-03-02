import logging
from datetime import datetime

from core.models import (
    FundamentalData,
    IndicatorResult,
    Signal,
    SignalType,
    StockQuote,
)
from engine.rule_engine import evaluate_rules
from indicators.fundamental import evaluate_fundamentals

logger = logging.getLogger(__name__)


class Recommender:
    def __init__(self, config: dict):
        self.rules = config.get("rules", {})
        self.sentiment_weight = config.get("sentiment", {}).get("weight", 0.3)

    def recommend(
        self,
        quote: StockQuote,
        indicators: IndicatorResult,
        fundamentals: FundamentalData | None = None,
        sentiment_score: float = 0.0,
    ) -> Signal:
        # 1. 규칙 기반 평가
        rule_signal, rule_reasons = evaluate_rules(indicators, self.rules)

        # 2. 펀더멘털 평가
        fundamental_reasons: list[str] = []
        if fundamentals:
            fundamental_reasons = evaluate_fundamentals(fundamentals, self.rules)

        # 3. 하이브리드 점수 계산
        all_reasons = rule_reasons + fundamental_reasons

        # 규칙 엔진 시그널 기반 점수
        if rule_signal == SignalType.BUY:
            rule_score = 1.0
        elif rule_signal == SignalType.SELL:
            rule_score = -1.0
        else:
            rule_score = 0.0

        # 펀더멘털 이유에서 추가 보정
        fund_buy = sum(1 for r in fundamental_reasons if r.startswith("BUY"))
        fund_sell = sum(1 for r in fundamental_reasons if r.startswith("SELL"))
        if fund_buy + fund_sell > 0:
            fund_score = (fund_buy - fund_sell) / (fund_buy + fund_sell)
            rule_score = (rule_score + fund_score) / 2

        # 감성 반영
        combined_score = (1 - self.sentiment_weight) * rule_score + self.sentiment_weight * sentiment_score

        # 최종 시그널 결정
        if combined_score > 0.2:
            final_signal = SignalType.BUY
        elif combined_score < -0.2:
            final_signal = SignalType.SELL
        else:
            final_signal = SignalType.HOLD

        confidence = min(abs(combined_score), 1.0)

        if sentiment_score != 0.0:
            all_reasons.append(f"Sentiment: {sentiment_score:+.2f}")

        return Signal(
            symbol=quote.symbol,
            signal_type=final_signal,
            confidence=confidence,
            reasons=all_reasons,
            price=quote.price,
            timestamp=datetime.now(),
            indicators=indicators,
            fundamentals=fundamentals,
            sentiment_score=sentiment_score,
        )
