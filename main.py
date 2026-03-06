import logging
import os
from datetime import datetime
from time import sleep

import yfinance as yf
from dotenv import load_dotenv

# Docker overlay filesystem에서 SQLite lock 방지: /dev/shm(ramdisk) 우선, fallback /tmp
_shm = "/dev/shm" if os.path.isdir("/dev/shm") else "/tmp"
_tz_cache_dir = os.path.join(_shm, f"yfinance_tz_cache_{os.getpid()}")
os.makedirs(_tz_cache_dir, exist_ok=True)
yf.set_tz_cache_location(_tz_cache_dir)

from core.models import NewsAlertItem, SignalType
from crawler.rss_fetcher import RSSFetcher
from crawler.rss_parser import RSSParser
from engine.news_evaluator import NewsEvaluator
from engine.recommender import Recommender
from engine.sentiment import SentimentAnalyzer
from indicators.technical import calculate_indicators
from providers.fundamental.yfinance_fundamental import YFinanceFundamentalProvider
from providers.news.rate_limiter import RateLimiter
from providers.news.rss_provider import RSSNewsProvider
from providers.price.yfinance_provider import YFinancePriceProvider
from screener.stock_screener import StockScreener
from sender.formatters import (
    format_discovery_message,
    format_news_alert_message,
    format_signal_message,
)
from sender.slack_sender import SlackSender
from sender.translator import GPTTranslator
from utils.config_loader import (
    get_discovery_config,
    get_watchlist,
    is_discovery_enabled,
    load_config,
)
from utils.dup_check import DuplicateChecker, deduplicate_similar
from utils.file_ctrl import save_file

# 소스별 타이틀 prefix 매핑
_SOURCE_PREFIXES = {"financialjuice": "FinancialJuice:"}

_log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=_log_level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# 써드파티 라이브러리는 DEBUG 모드에서도 WARNING 이상만 출력
for _noisy_logger in ("yfinance", "httpx", "urllib3", "requests", "peewee"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def _init_news_store(config: dict):
    """DB 설정이 있으면 NewsStore를 초기화, 없으면 None 반환"""
    db_cfg = config.get("database", {})
    if not db_cfg.get("enabled", False):
        return None

    try:
        from storage.news_store import NewsStore

        db_path = db_cfg.get("path", "./data/news.duckdb")
        store = NewsStore(db_path=db_path)
        store.init_schema()
        logger.info("DuckDB news store initialized: %s", db_path)
        return store
    except Exception as e:
        logger.warning("DB initialization failed, continuing without DB: %s", e)
        return None


def run_news_pipeline(
    config: dict,
    dup_checker: DuplicateChecker,
    news_store=None,
    watchlist: list[str] | None = None,
) -> tuple[list[str], dict[str, list[NewsAlertItem]]]:
    """RSS 뉴스 크롤링 → 번역 → 감성점수 계산 → DB 저장 + 파일 저장 파이프라인.

    2단계 구조:
      Phase 1 - 각 소스에서 수집 + 중복 제거
      Phase 2 - 전체 신규 건을 통합 배치로 번역/감성분석/저장

    Returns:
        (all_headlines, symbol_news_map) 튜플
        symbol_news_map: {symbol: [NewsAlertItem, ...]} — 관련 종목별 뉴스+감성
    """
    news_configs = config.get("providers", {}).get("news", [])
    all_headlines: list[str] = []
    symbol_news_map: dict[str, list[NewsAlertItem]] = {}

    sentiment_cfg = config.get("sentiment", {})
    sentiment_analyzer = None
    if sentiment_cfg.get("enabled"):
        sentiment_analyzer = SentimentAnalyzer(model=sentiment_cfg.get("model", "gpt-4o-mini"))

    # ── Phase 1: 각 소스에서 수집 + 중복 제거 ──
    # 수집 결과를 중간 구조로 모은다
    collected: list[dict] = []  # {source, news_item, cleaned_title, file_path}

    for news_cfg in news_configs:
        url = news_cfg.get("url")
        source = news_cfg.get("source", "unknown")
        if not url:
            continue

        try:
            provider = RSSNewsProvider(url)
            news_items = provider.fetch_news()
            items_with_title = [item for item in news_items if item.get("title")]

            prefix = _SOURCE_PREFIXES.get(source, "")
            titles = [item["title"].removeprefix(prefix).strip() for item in items_with_title]
            all_headlines.extend(titles)

            # 원문 기준 중복 체크
            dup_result = dup_checker.check(titles)
            new_orig_set = set(dup_result["new"].keys())
            logger.info(
                "[%s] %d new, %d duplicate",
                source, len(dup_result["new"]), len(dup_result["duplicate"]),
            )

            if not dup_result["new"]:
                continue

            # 신규 건만 필터링
            for item, title in zip(items_with_title, titles):
                if title in new_orig_set:
                    collected.append({
                        "source": source,
                        "news_item": item,
                        "cleaned_title": title,
                        "file_path": dup_result["new"].get(title),
                    })

        except Exception as e:
            logger.error("[%s] News fetch error: %s", source, e)

    if not collected:
        return all_headlines, symbol_news_map

    # ── Phase 1.5: 유사 뉴스 중복 제거 ──
    dedup_cfg = config.get("news_dedup", {})
    similarity_threshold = dedup_cfg.get("similarity_threshold", 0.65)
    collected = deduplicate_similar(collected, threshold=similarity_threshold)

    if not collected:
        return all_headlines, symbol_news_map

    # ── Phase 2: 통합 배치 처리 ──
    embed_map: dict[str, list[float]] = {
        c["cleaned_title"]: c["embedding"]
        for c in collected
        if c.get("embedding") is not None
    }
    all_new_titles = [c["cleaned_title"] for c in collected]
    logger.info("Phase 2: processing %d new items from %d sources",
                len(all_new_titles), len({c["source"] for c in collected}))

    # 2-1. 번역 + 카테고리 태깅 + 심볼 추출 (GPT 배치 1회)
    translator = GPTTranslator()
    all_translated, all_categories, all_symbols_per_title = translator.translate_and_categorize_titles(all_new_titles)
    trans_map = dict(zip(all_new_titles, all_translated))
    cat_map = dict(zip(all_new_titles, all_categories))
    gpt_symbol_map = dict(zip(all_new_titles, all_symbols_per_title))

    # 2-2. 감성점수 배치 계산
    if sentiment_analyzer:
        raw_scores = sentiment_analyzer.analyze_batch(all_new_titles)
        all_scores: list[float | None] = list(raw_scores)
    else:
        all_scores = [None] * len(all_new_titles)
    score_map = dict(zip(all_new_titles, all_scores))

    # 2-3. 파일 저장
    for c in collected:
        if c["file_path"]:
            title = c["cleaned_title"]
            save_file(file_path=c["file_path"], title=trans_map.get(title, title))

    # 2-4. Slack 발송
    news_channel = os.getenv("SLACK_CHANNEL_NEWS")
    if news_channel:
        try:
            slack = SlackSender()
            lines = []
            for title in all_new_titles:
                translated = trans_map.get(title, title)
                cats = cat_map.get(title, [])
                tag = " ".join(f"[{c}]" for c in cats) if cats else ""
                lines.append(f"• {tag} {translated}" if tag else f"• {translated}")
            msg = "\n".join(lines)
            slack.send_bot_message(channel=news_channel, message=msg)
        except Exception as e:
            logger.error("News channel Slack send failed: %s", e)

    # 2-5. DB 저장 (소스별로 묶어서 저장)
    if news_store is not None:
        # 소스별로 그룹핑
        from itertools import groupby
        from operator import itemgetter

        for source, group in groupby(collected, key=itemgetter("source")):
            items_in_source = list(group)
            src_news_items = [c["news_item"] for c in items_in_source]
            src_titles = [c["cleaned_title"] for c in items_in_source]
            src_translated = [trans_map.get(t, t) for t in src_titles]
            src_scores = [score_map.get(t) for t in src_titles]
            src_categories = [cat_map.get(t, []) for t in src_titles]
            src_embeddings = [embed_map.get(t) for t in src_titles]
            src_symbols = [gpt_symbol_map.get(t, []) for t in src_titles]
            _save_news_to_db(
                news_store, src_news_items, src_titles, src_translated,
                source, watchlist, src_scores, src_categories, src_embeddings, src_symbols,
            )

    # 2-6. symbol_news_map 구축 (GPT 추출 심볼 사용, watchlist 필터링)
    watchlist_upper = {s.upper() for s in watchlist} if watchlist else set()

    for title in all_new_titles:
        score = score_map.get(title)
        if score is None:
            continue
        gpt_symbols = gpt_symbol_map.get(title, [])
        # watchlist에 있는 심볼만 알림 대상으로 사용
        related = [s for s in gpt_symbols if not watchlist_upper or s in watchlist_upper]
        for sym in related:
            symbol_news_map.setdefault(sym, []).append(
                NewsAlertItem(title=title, sentiment_score=score)
            )

    return all_headlines, symbol_news_map


def _save_news_to_db(
    news_store, news_items, titles, translated, source, watchlist,
    sentiment_scores: list[float | None] | None = None,
    categories_list: list[list[str]] | None = None,
    embeddings: list[list[float] | None] | None = None,
    symbols_list: list[list[str]] | None = None,
) -> set[str]:
    """수집한 뉴스를 DB에 저장. 감성점수 + 카테고리 + 심볼도 즉시 포함.

    Returns:
        관련 종목 심볼 집합
    """
    from storage.models import NewsRecord

    records = []
    all_symbols: set[str] = set()
    for i, item in enumerate(news_items):
        original = titles[i] if i < len(titles) else item.get("title", "")
        trans = translated[i] if i < len(translated) else original

        # GPT가 추출한 심볼 사용
        related = symbols_list[i] if symbols_list and i < len(symbols_list) else []
        all_symbols.update(related)

        pub_date = None
        if item.get("pub_date"):
            try:
                pub_date = datetime.fromisoformat(item["pub_date"])
            except (ValueError, TypeError):
                pass

        score = sentiment_scores[i] if sentiment_scores and i < len(sentiment_scores) else None
        cats = categories_list[i] if categories_list and i < len(categories_list) else None
        emb = embeddings[i] if embeddings and i < len(embeddings) else None

        records.append(NewsRecord(
            title_original=original,
            title_translated=trans,
            source=source,
            link=item.get("link", ""),
            published_at=pub_date,
            collected_at=datetime.now(),
            sentiment_score=score,
            related_symbols=related if related else None,
            categories=cats,
            embedding=emb,
        ))

    news_store.save_news_batch(records)
    return all_symbols


def _get_historical_headlines(news_store, symbol: str, days: int = 7) -> list[str]:
    """DB에서 종목 관련 과거 뉴스 헤드라인 조회"""
    if news_store is None:
        return []

    try:
        records = news_store.search_by_symbol(symbol, days=days, limit=20)
        if not records:
            # 심볼 매칭이 없으면 키워드로 검색
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

            # 감성 분석: 현재 헤드라인 + 과거 뉴스 결합
            sentiment_score = 0.0
            if sentiment_analyzer:
                # 현재 수집된 헤드라인
                current_headlines = headlines[:10] if headlines else []

                # DB에서 종목별 과거 뉴스 조회
                historical = _get_historical_headlines(news_store, symbol, days=7)

                # 현재 + 과거 결합 (현재 뉴스 우선)
                combined = current_headlines + historical
                if combined:
                    sentiment_score = sentiment_analyzer.analyze(combined[:20])

                    # 감성 점수를 DB에 역으로 저장 (최근 뉴스에 대해)
                    if news_store and historical:
                        for record in news_store.search_by_symbol(symbol, days=1, limit=5):
                            if record.sentiment_score is None and record.id:
                                news_store.update_sentiment(record.id, sentiment_score)

            signal = recommender.recommend(quote, indicators, fundamentals, sentiment_score)
            signals.append(signal)
            logger.info("%s: %s (confidence=%.0f%%)", symbol, signal.signal_type.value, signal.confidence * 100)
        except Exception as e:
            logger.error("Analysis failed for %s: %s", symbol, e)

    return signals


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

            # 중복 알림 방지
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


def backfill_categories(news_store, delay: float = 1.0) -> int:
    """기존 뉴스에 카테고리를 소급 적용합니다."""
    uncategorized = news_store.get_uncategorized_news()
    if not uncategorized:
        logger.info("No uncategorized news found")
        return 0

    logger.info("Backfilling categories for %d news items", len(uncategorized))
    translator = GPTTranslator()
    count = 0
    for record in uncategorized:
        try:
            _, categories = translator.translate_and_categorize(record.title_original)
            news_store.update_categories(record.id, categories)
            count += 1
            logger.info(
                "Backfill (%d/%d) id=%d: [%s] %s",
                count, len(uncategorized), record.id,
                ", ".join(categories), record.title_original[:60],
            )
            if count < len(uncategorized):
                sleep(delay)
        except Exception as e:
            logger.error("Backfill failed for id=%d: %s", record.id, e)

    logger.info("Backfill complete: %d/%d categorized", count, len(uncategorized))
    return count


def main():
    load_dotenv()
    config = load_config()

    dup_checker = DuplicateChecker()
    poll_interval = config.get("poll_interval_seconds", 300)
    rate_limiter = RateLimiter(state_file="./data/rate_limiter_state.json")

    # DB 초기화
    news_store = _init_news_store(config)

    # 종목 탐색은 첫 실행 시 한 번만
    watchlist = get_watchlist(config)
    discovered_symbols = discover_new_stocks(config, watchlist)
    all_symbols = list(dict.fromkeys(watchlist + discovered_symbols))
    logger.info("Tracking %d symbols: %s", len(all_symbols), all_symbols)

    while True:
        try:
            # Docker 재시작 등으로 인한 429 방지: 마지막 실행 이후 남은 대기 시간만큼 대기
            rate_limiter.wait_if_needed("news_pipeline", poll_interval)

            # 1. 뉴스 파이프라인 (감성점수 즉시 계산 + DB 저장)
            headlines, symbol_news_map = run_news_pipeline(config, dup_checker, news_store, all_symbols)

            # 2. 뉴스 트리거 기반 즉시 분석 & 알림
            if symbol_news_map:
                run_news_evaluation(config, symbol_news_map, dup_checker)

            # 3. 주식 분석 파이프라인 (과거 뉴스 DB 활용)
            signals = run_stock_pipeline(config, all_symbols, headlines, news_store)

            # 4. 시그널 Slack 전송 (HOLD 제외, 중복 제외)
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

            # 5. 페이퍼 트레이딩 (활성화 시)
            if config.get("paper_trading", {}).get("enabled"):
                try:
                    from portfolio.paper_trader import PaperTrader

                    trader = PaperTrader(config["paper_trading"])
                    for signal in signals:
                        if signal.signal_type != SignalType.HOLD:
                            trader.execute_signal(signal)
                    trader.save()
                except Exception as e:
                    logger.error("Paper trading error: %s", e)

        except Exception as e:
            logger.error("Pipeline error: %s", e)

        logger.info("Sleeping %d seconds...", poll_interval)
        sleep(poll_interval)


if __name__ == "__main__":
    import sys

    if "--backfill-categories" in sys.argv:
        load_dotenv()
        config = load_config()
        news_store = _init_news_store(config)
        if news_store is None:
            logger.error("Database not enabled in config")
            sys.exit(1)
        backfill_categories(news_store)
        news_store.close()
    else:
        main()
