import logging
import os
import sys

import yfinance as yf
from dotenv import load_dotenv

# Docker overlay filesystem에서 SQLite lock 방지: /dev/shm(ramdisk) 우선, fallback /tmp
_shm = "/dev/shm" if os.path.isdir("/dev/shm") else "/tmp"
_tz_cache_dir = os.path.join(_shm, f"yfinance_tz_cache_{os.getpid()}")
os.makedirs(_tz_cache_dir, exist_ok=True)
yf.set_tz_cache_location(_tz_cache_dir)

from shared.config import get_watchlist, load_config
from shared.db import init_news_store, init_stock_store
from shared.models import SignalType
from shared.slack import SlackSender

from news.dedup import DuplicateChecker
from news.pipeline import backfill_categories, run_news_pipeline
from news.rate_limiter import RateLimiter

from alerts.pipeline import run_news_evaluation
from analysis.formatter import format_signal_message, format_macro_message
from analysis.macro import MacroDataProvider
from analysis.pipeline import run_stock_pipeline
from discovery.pipeline import discover_new_stocks

_log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=_log_level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

for _noisy_logger in ("yfinance", "httpx", "urllib3", "requests", "peewee"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    load_dotenv()
    config = load_config()

    dup_checker = DuplicateChecker()
    poll_interval = config.get("poll_interval_seconds", 300)
    rate_limiter = RateLimiter(state_file="./data/rate_limiter_state.json")

    news_store = init_news_store(config)
    stock_store = init_stock_store(config)

    # 월드 컨텍스트 초기화
    world_context_provider = None
    world_ctx_cfg = config.get("world_context", {})
    if world_ctx_cfg.get("enabled"):
        from news.world_context import WorldContextProvider
        world_context_provider = WorldContextProvider(
            model=world_ctx_cfg.get("model", "gpt-4o-mini"),
            cache_hours=world_ctx_cfg.get("cache_hours", 24),
        )

    # 매크로 데이터 프로바이더 초기화
    macro_provider = MacroDataProvider()

    # 종목 탐색 (첫 실행 시 한 번)
    watchlist = get_watchlist(config)
    discovered_symbols = discover_new_stocks(config, watchlist)
    all_symbols = list(dict.fromkeys(watchlist + discovered_symbols))
    logger.info("Tracking %d symbols: %s", len(all_symbols), all_symbols)

    while True:
        try:
            rate_limiter.wait_if_needed("news_pipeline", poll_interval)

            # 1. 뉴스 파이프라인
            headlines, symbol_news_map = run_news_pipeline(
                config, dup_checker, news_store, all_symbols, world_context_provider
            )

            # 2. 매크로 환경 데이터 수집
            macro_data = None
            try:
                macro_data = macro_provider.get_macro_data()
                if not dup_checker.check_signal_duplicate("MACRO", "macro_update"):
                    slack = SlackSender()
                    macro_msg = format_macro_message(macro_data)
                    slack.send_webhook_message(macro_msg)
                    dup_checker.mark_signal_sent("MACRO", "macro_update")
                    logger.info("Macro data sent to Slack")
            except Exception as e:
                logger.warning("Macro data collection failed: %s", e)

            # 3. 뉴스 트리거 알림
            if symbol_news_map:
                run_news_evaluation(config, symbol_news_map, dup_checker)

            # 4. 주식 분석
            signals = run_stock_pipeline(config, all_symbols, headlines, news_store, stock_store)

            # 5. 시그널 Slack 전송
            for signal in signals:
                if signal.signal_type == SignalType.HOLD:
                    continue
                if dup_checker.check_signal_duplicate(signal.symbol, signal.signal_type.value):
                    continue
                try:
                    slack = SlackSender()
                    msg = format_signal_message(signal)
                    slack.send_webhook_message(msg)
                    dup_checker.mark_signal_sent(signal.symbol, signal.signal_type.value)
                    logger.info("Signal sent: %s %s", signal.signal_type.value, signal.symbol)
                except Exception as e:
                    logger.error("Slack send failed: %s", e)

            # 6. 페이퍼 트레이딩
            if config.get("paper_trading", {}).get("enabled"):
                try:
                    from trading.paper_trader import PaperTrader
                    trader = PaperTrader(config["paper_trading"])
                    for signal in signals:
                        if signal.signal_type != SignalType.HOLD:
                            trader.execute_signal(signal)
                    trader.save()
                except Exception as e:
                    logger.error("Paper trading error: %s", e)

        except Exception as e:
            logger.error("Pipeline error: %s", e)


if __name__ == "__main__":
    if "--backfill-categories" in sys.argv:
        load_dotenv()
        config = load_config()
        news_store = init_news_store(config)
        if news_store is None:
            logger.error("Database not enabled in config")
            sys.exit(1)
        backfill_categories(news_store)
        news_store.close()
    else:
        main()
