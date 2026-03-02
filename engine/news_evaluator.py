"""뉴스 기반 종목 평가 엔진.

뉴스 감성점수와 기술/펀더멘털 지표를 결합하여
저평가/고평가 판단 및 알림을 생성한다.
"""

import logging
from typing import Optional

from core.models import (
    FundamentalData,
    IndicatorResult,
    NewsAlert,
    NewsAlertItem,
)

logger = logging.getLogger(__name__)


class NewsEvaluator:
    def __init__(self, config: dict):
        eval_cfg = config.get("news_evaluation", {})
        self.enabled = eval_cfg.get("enabled", False)
        self.thresholds = eval_cfg.get("thresholds", {})
        self.weights = eval_cfg.get("weights", {})
        self.alert_threshold = eval_cfg.get("alert_threshold", 0.4)

    def _normalize_rsi(self, rsi: Optional[float]) -> Optional[float]:
        """RSI를 [-1, +1] 범위로 정규화.

        undervalued(35) 미만 → -1 방향, overvalued(65) 초과 → +1 방향, 중간(50)=0
        """
        if rsi is None:
            return None
        cfg = self.thresholds.get("rsi", {})
        under = cfg.get("undervalued", 35)
        over = cfg.get("overvalued", 65)
        mid = (under + over) / 2

        if rsi <= under:
            return max(-1.0, -(mid - rsi) / (mid - 0))
        if rsi >= over:
            return min(1.0, (rsi - mid) / (100 - mid))
        # 중간 구간: 선형 보간 [-1, +1] 사이
        if rsi < mid:
            return -(mid - rsi) / (mid - under)
        return (rsi - mid) / (over - mid)

    def _normalize_ratio(self, value: Optional[float], indicator: str) -> Optional[float]:
        """PER/PBR 등 비율 지표를 [-1, +1]로 정규화."""
        if value is None:
            return None
        cfg = self.thresholds.get(indicator, {})
        under = cfg.get("undervalued")
        over = cfg.get("overvalued")
        if under is None or over is None:
            return None

        mid = (under + over) / 2
        if value <= under:
            # under 이하 → -1 방향 (선형, 최소 -1)
            return max(-1.0, -(mid - value) / (mid - 0) if mid > 0 else -1.0)
        if value >= over:
            # over 이상 → +1 방향 (선형, 최대 +1)
            return min(1.0, (value - mid) / mid if mid > 0 else 1.0)
        if value < mid:
            return -(mid - value) / (mid - under)
        return (value - mid) / (over - mid)

    def _normalize_bollinger(
        self,
        price: float,
        upper: Optional[float],
        middle: Optional[float],
        lower: Optional[float],
    ) -> Optional[float]:
        """볼린저 밴드 내 위치 기반 정규화."""
        bb_cfg = self.thresholds.get("bollinger", {})
        if not bb_cfg.get("enabled", True):
            return None
        if upper is None or middle is None or lower is None:
            return None

        if price <= lower:
            return -1.0
        if price >= upper:
            return 1.0
        # 밴드 내 위치를 [-1, +1]로 매핑
        if price < middle:
            return -(middle - price) / (middle - lower) if (middle - lower) > 0 else 0.0
        return (price - middle) / (upper - middle) if (upper - middle) > 0 else 0.0

    def compute_composite_score(
        self,
        price: float,
        indicators: Optional[IndicatorResult],
        fundamentals: Optional[FundamentalData],
    ) -> tuple[float, dict]:
        """종합 평가 점수 계산.

        Returns:
            (composite_score, indicator_details) 튜플
            composite_score: -1.0 ~ +1.0
            indicator_details: 지표별 {raw, normalized, weight} 딕셔너리
        """
        scores: dict[str, Optional[float]] = {}
        details: dict[str, dict] = {}

        # RSI
        rsi_raw = indicators.rsi if indicators else None
        scores["rsi"] = self._normalize_rsi(rsi_raw)
        details["rsi"] = {"raw": rsi_raw, "normalized": scores["rsi"], "weight": self.weights.get("rsi", 0.25)}

        # PER
        per_raw = fundamentals.per if fundamentals else None
        scores["per"] = self._normalize_ratio(per_raw, "per")
        details["per"] = {"raw": per_raw, "normalized": scores["per"], "weight": self.weights.get("per", 0.30)}

        # PBR
        pbr_raw = fundamentals.pbr if fundamentals else None
        scores["pbr"] = self._normalize_ratio(pbr_raw, "pbr")
        details["pbr"] = {"raw": pbr_raw, "normalized": scores["pbr"], "weight": self.weights.get("pbr", 0.20)}

        # Bollinger
        bb_upper = indicators.bollinger_upper if indicators else None
        bb_middle = indicators.bollinger_middle if indicators else None
        bb_lower = indicators.bollinger_lower if indicators else None
        scores["bollinger"] = self._normalize_bollinger(price, bb_upper, bb_middle, bb_lower)
        details["bollinger"] = {
            "raw": {"upper": bb_upper, "middle": bb_middle, "lower": bb_lower, "price": price},
            "normalized": scores["bollinger"],
            "weight": self.weights.get("bollinger", 0.25),
        }

        # 가중 합산 (None인 지표 제외, 가중치 재정규화)
        total_weight = 0.0
        weighted_sum = 0.0
        for key, norm_score in scores.items():
            if norm_score is not None:
                w = self.weights.get(key, 0.0)
                weighted_sum += norm_score * w
                total_weight += w

        composite = weighted_sum / total_weight if total_weight > 0 else 0.0
        return composite, details

    def _determine_conclusion(self, avg_sentiment: float, composite: float) -> str:
        """뉴스 감성과 지표 교차 판단."""
        is_positive = avg_sentiment > 0
        is_undervalued = composite < -self.alert_threshold
        is_overvalued = composite > self.alert_threshold

        if is_positive and is_undervalued:
            return "호재 뉴스 + 저평가 → 매수 주목 :fire:"
        if not is_positive and is_overvalued:
            return "악재 뉴스 + 고평가 → 매도 주의 :warning:"
        if is_positive and is_overvalued:
            return "호재 뉴스 + 고평가 → 이미 반영됨, 관망"
        if not is_positive and is_undervalued:
            return "악재 뉴스 + 저평가 → 추가 하락 가능, 주의"
        return "뉴스 감성과 지표가 중립적 → 관망"

    def evaluate(
        self,
        symbol: str,
        price: float,
        news_items: list[NewsAlertItem],
        indicators: Optional[IndicatorResult],
        fundamentals: Optional[FundamentalData],
    ) -> Optional[NewsAlert]:
        """뉴스 트리거 종목 평가 수행.

        composite_score 절대값이 alert_threshold를 넘으면 NewsAlert 반환,
        아니면 None.
        """
        if not self.enabled or not news_items:
            return None

        composite, details = self.compute_composite_score(price, indicators, fundamentals)

        # 알림 기준 미달 시 스킵
        if abs(composite) < self.alert_threshold:
            logger.debug("%s: composite=%.2f, below threshold %.2f", symbol, composite, self.alert_threshold)
            return None

        avg_sentiment = sum(n.sentiment_score for n in news_items) / len(news_items)

        if composite < -self.alert_threshold:
            valuation = "저평가"
        elif composite > self.alert_threshold:
            valuation = "고평가"
        else:
            valuation = "적정"

        conclusion = self._determine_conclusion(avg_sentiment, composite)

        return NewsAlert(
            symbol=symbol,
            price=price,
            news_items=news_items,
            composite_score=composite,
            indicator_scores=details,
            valuation=valuation,
            conclusion=conclusion,
        )
