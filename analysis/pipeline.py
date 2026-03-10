"""주식 분석 + 추천 파이프라인."""

import logging

from analysis.providers import YFinanceFundamentalProvider, YFinancePriceProvider
from analysis.recommender import Recommender
from analysis.technical import calculate_indicators
from news.sentiment import SentimentAnalyzer

logger = logging.getLogger(__name__)


def _get_historical_headlines(news_store, symbol: str, days: int = 7) -> list[str]:
    """DB에서 종목 관련 과거 뉴스 헤드라인 조회"""
    if news_store is None:
        return []

    try:
        records = news_store.search_by_symbol(symbol, days=days, limit=20)
        if not records:
            records = news_store.search_by_keyword(symbol, days=days, limit=20)
        return [r.title_original for r in records]
    except Exception as e:
        logger.debug("Historical news lookup failed for %s: %s", symbol, e)
        return []


def run_stock_pipeline(
    config: dict,
    symbols: list[str],
    headlines: list[str],
    news_store=None,
    stock_store=None,
) -> list:
    """주식 분석 + 추천 파이프라인 (과거 뉴스 데이터 활용)"""
    price_provider = YFinancePriceProvider()
    fundamental_provider = YFinanceFundamentalProvider()
    recommender = Recommender(config)

    sentiment_cfg = config.get("sentiment", {})
    sentiment_analyzer = None
    if sentiment_cfg.get("enabled"):
        sentiment_analyzer = SentimentAnalyzer(model=sentiment_cfg.get("model", "gpt-4o-mini"))

    signals = []
    for symbol in symbols:
        try:
            quote = price_provider.get_current_price(symbol)
            hist = price_provider.get_historical(symbol, period="6mo")
            indicators = calculate_indicators(hist, symbol, config)

            fundamentals = None
            try:
                fundamentals = fundamental_provider.get_fundamentals(symbol)
            except Exception as e:
                logger.warning("Fundamentals failed for %s: %s", symbol, e)

            sentiment_score = 0.0
            if sentiment_analyzer:
                current_headlines = headlines[:10] if headlines else []
                historical = _get_historical_headlines(news_store, symbol, days=7)
                combined = current_headlines + historical
                if combined:
                    sentiment_score = sentiment_analyzer.analyze(combined[:20])

                    if news_store and historical:
                        for record in news_store.search_by_symbol(symbol, days=1, limit=5):
                            if record.sentiment_score is None and record.id:
                                news_store.update_sentiment(record.id, sentiment_score)

            signal = recommender.recommend(quote, indicators, fundamentals, sentiment_score)
            signals.append(signal)
            logger.info("%s: %s (confidence=%.0f%%)", symbol, signal.signal_type.value, signal.confidence * 100)

            if stock_store is not None:
                try:
                    stock_store.save_snapshot(
                        symbol=symbol,
                        date=quote.timestamp,
                        quote=quote,
                        indicators=indicators,
                        fundamentals=fundamentals,
                    )
                except Exception as e:
                    logger.warning("Snapshot save failed for %s: %s", symbol, e)
        except Exception as e:
            logger.error("Analysis failed for %s: %s", symbol, e)

    return signals
