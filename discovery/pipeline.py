"""종목 자동 탐색 파이프라인."""

import logging

from shared.config import get_discovery_config, is_discovery_enabled
from shared.slack import SlackSender

from discovery.formatter import format_discovery_message
from discovery.screener import StockScreener

logger = logging.getLogger(__name__)


def discover_new_stocks(config: dict, watchlist: list[str] | None = None) -> list[str]:
    """종목 자동 탐색 — 새로운 종목을 발견하여 심볼 리스트 반환"""
    if not is_discovery_enabled(config):
        return []

    discovery_cfg = get_discovery_config(config)
    screener = StockScreener(discovery_cfg)

    try:
        discovered = screener.discover_stocks(extra_symbols=watchlist)
        logger.info("Discovered %d new stock candidates", len(discovered))

        try:
            slack = SlackSender()
            msg = format_discovery_message(discovered)
            if msg:
                slack.send_webhook_message(msg)
        except Exception:
            pass

        return [d.symbol for d in discovered]
    except Exception as e:
        logger.error("Stock discovery failed: %s", e)
        return []
