from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class SignalType(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class StockQuote:
    symbol: str
    price: float
    open: float
    high: float
    low: float
    volume: int
    timestamp: datetime


@dataclass
class FundamentalData:
    symbol: str
    per: Optional[float] = None
    pbr: Optional[float] = None
    eps: Optional[float] = None
    market_cap: Optional[float] = None
    dividend_yield: Optional[float] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    psr: Optional[float] = None
    roe: Optional[float] = None
    debt_to_equity: Optional[float] = None


@dataclass
class IndicatorResult:
    symbol: str
    rsi: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    sma: dict = field(default_factory=dict)
    bollinger_upper: Optional[float] = None
    bollinger_middle: Optional[float] = None
    bollinger_lower: Optional[float] = None


@dataclass
class Signal:
    symbol: str
    signal_type: SignalType
    confidence: float  # 0.0 ~ 1.0
    reasons: list[str]
    price: float
    timestamp: datetime
    indicators: Optional[IndicatorResult] = None
    fundamentals: Optional[FundamentalData] = None
    sentiment_score: Optional[float] = None


@dataclass
class ScreenerResult:
    symbol: str
    name: str
    sector: Optional[str] = None
    market_cap: Optional[float] = None
    volume: Optional[int] = None
    price: Optional[float] = None
    change_pct: Optional[float] = None
    discovery_reason: str = ""


@dataclass
class NewsAlertItem:
    title: str
    sentiment_score: float


@dataclass
class NewsAlert:
    symbol: str
    price: float
    news_items: list[NewsAlertItem]
    composite_score: float  # -1.0 (강한 저평가) ~ +1.0 (강한 고평가)
    indicator_scores: dict[str, Any]  # 지표별 {raw, normalized, weight}
    valuation: str  # "저평가" / "고평가" / "적정"
    conclusion: str  # 종합 결론 문자열
