"""뉴스 트리거 기반 종목 평가 → Slack 알림 파이프라인."""

import logging

from shared.models import NewsAlertItem
from shared.slack import SlackSender

from alerts.evaluator import NewsEvaluator
from alerts.formatter import format_news_alert_message
from analysis.providers import YFinanceFundamentalProvider, YFinancePriceProvider
from analysis.technical import calculate_indicators
from news.dedup import DuplicateChecker

logger = logging.getLogger(__name__)


def run_news_evaluation(
    config: dict,
    symbol_news_map: dict[str, list[NewsAlertItem]],
    dup_checker: DuplicateChecker,
) -> list:
    """뉴스 트리거 기반 종목 평가 → Slack 알림 발송."""
    evaluator = NewsEvaluator(config)
    if not evaluator.enabled or not symbol_news_map:
        return []

    price_provider = YFinancePriceProvider()
    fundamental_provider = YFinanceFundamentalProvider()
    alerts = []

    for symbol, news_items in symbol_news_map.items():
        try:
            quote = price_provider.get_current_price(symbol)
            hist = price_provider.get_historical(symbol, period="6mo")
            indicators = calculate_indicators(hist, symbol, config)

            fundamentals = None
            try:
                fundamentals = fundamental_provider.get_fundamentals(symbol)
            except Exception as e:
                logger.warning("Fundamentals failed for %s: %s", symbol, e)

            alert = evaluator.evaluate(symbol, quote.price, news_items, indicators, fundamentals)
            if alert is None:
                continue

            alert_key = f"news_{alert.valuation}"
            if dup_checker.check_signal_duplicate(symbol, alert_key):
                logger.debug("Duplicate news alert skipped: %s %s", symbol, alert_key)
                continue

            alerts.append(alert)

            try:
                slack = SlackSender()
                msg = format_news_alert_message(alert)
                slack.send_webhook_message(msg)
                dup_checker.mark_signal_sent(symbol, alert_key)
                logger.info("News alert sent: %s (%s)", symbol, alert.valuation)
            except Exception as e:
                logger.error("News alert Slack send failed: %s", e)

        except Exception as e:
            logger.error("News evaluation failed for %s: %s", symbol, e)

    return alerts
