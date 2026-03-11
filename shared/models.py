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
    # 확장 펀더멘털
    forward_pe: Optional[float] = None
    peg_ratio: Optional[float] = None
    ev_to_ebitda: Optional[float] = None
    ev_to_revenue: Optional[float] = None
    fcf: Optional[float] = None  # Free Cash Flow
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    fcf_margin: Optional[float] = None
    roic: Optional[float] = None
    revenue_cagr_5y: Optional[float] = None
    eps_cagr_5y: Optional[float] = None
    insider_pct: Optional[float] = None
    institutional_pct: Optional[float] = None
    short_pct_of_float: Optional[float] = None
    # 밸류에이션/애널리스트
    target_mean_price: Optional[float] = None
    target_high_price: Optional[float] = None
    target_low_price: Optional[float] = None
    recommendation: Optional[str] = None  # buy/hold/sell
    num_analyst_opinions: Optional[int] = None
    # 어닝스
    next_earnings_date: Optional[str] = None


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
    # 확장 기술적 지표
    ema: dict = field(default_factory=dict)  # {period: value}
    obv: Optional[float] = None
    golden_cross: Optional[bool] = None  # SMA50 > SMA200 교차 발생
    death_cross: Optional[bool] = None   # SMA50 < SMA200 교차 발생
    support: Optional[float] = None      # 피봇 지지선
    resistance: Optional[float] = None   # 피봇 저항선
    pivot: Optional[float] = None        # 피봇 포인트


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
class MacroData:
    """매크로 시장 환경 데이터"""
    vix: Optional[float] = None
    treasury_10y: Optional[float] = None
    treasury_2y: Optional[float] = None
    yield_spread_10y_2y: Optional[float] = None
    dxy: Optional[float] = None  # 달러 인덱스
    wti_oil: Optional[float] = None
    gold: Optional[float] = None
    sp500: Optional[float] = None
    sp500_sma200: Optional[float] = None
    sp500_above_sma200: Optional[bool] = None  # 시장 레짐
    fed_funds_rate: Optional[float] = None
    timestamp: Optional[datetime] = None


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
