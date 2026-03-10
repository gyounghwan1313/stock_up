"""뉴스 수집 & 큐레이션 파이프라인."""

import logging
import os
from datetime import datetime
from time import sleep

from shared.models import NewsAlertItem
from shared.slack import SlackSender

from news.curator import CuratedGroup, NewsCurator
from news.dedup import DuplicateChecker, deduplicate_similar
from news.file_ctrl import save_file
from news.rss_provider import RSSNewsProvider
from news.sentiment import SentimentAnalyzer
from news.store import NewsRecord
from news.symbol_extractor import extract_symbols
from news.translator import GPTTranslator

logger = logging.getLogger(__name__)

# 소스별 타이틀 prefix 매핑
_SOURCE_PREFIXES = {"financialjuice": "FinancialJuice:"}


def run_news_pipeline(
    config: dict,
    dup_checker: DuplicateChecker,
    news_store=None,
    watchlist: list[str] | None = None,
    world_context_provider=None,
) -> tuple[list[str], dict[str, list[NewsAlertItem]]]:
    """RSS 뉴스 크롤링 → 큐레이션 → 감성점수 → DB 저장 파이프라인.

    Returns:
        (all_headlines, symbol_news_map) 튜플
    """
    news_configs = config.get("providers", {}).get("news", [])
    all_headlines: list[str] = []
    symbol_news_map: dict[str, list[NewsAlertItem]] = {}

    sentiment_cfg = config.get("sentiment", {})
    sentiment_analyzer = None
    if sentiment_cfg.get("enabled"):
        sentiment_analyzer = SentimentAnalyzer(model=sentiment_cfg.get("model", "gpt-4o-mini"))

    # ── Phase 1: 각 소스에서 수집 + 중복 제거 ──
    collected: list[dict] = []

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

            dup_result = dup_checker.check(titles)
            new_orig_set = set(dup_result["new"].keys())
            logger.info(
                "[%s] %d new, %d duplicate",
                source, len(dup_result["new"]), len(dup_result["duplicate"]),
            )

            if not dup_result["new"]:
                continue

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

    # 2-1. LLM 큐레이션
    curator_cfg = config.get("news_curator", {})
    world_ctx = ""
    if world_context_provider:
        world_ctx = world_context_provider.get_context()

    if curator_cfg.get("enabled"):
        curator = NewsCurator(
            model=curator_cfg.get("model", "gpt-4o-mini"),
            importance_threshold=curator_cfg.get("importance_threshold", 6),
            batch_size=curator_cfg.get("batch_size", 30),
        )
        curated_groups = curator.curate(all_new_titles, world_ctx)
    else:
        import uuid
        translator = GPTTranslator()
        all_translated, all_categories, all_symbols = translator.translate_and_categorize_titles(all_new_titles)
        curated_groups = []
        for i, title in enumerate(all_new_titles):
            curated_groups.append(CuratedGroup(
                group_id=str(uuid.uuid4())[:8],
                headline_indices=[i],
                summary=all_translated[i],
                category=all_categories[i][0] if all_categories[i] else "기타",
                importance=5,
                reason="curator disabled",
                symbols=all_symbols[i] if i < len(all_symbols) else [],
            ))

    # 큐레이션 결과를 개별 헤드라인에 매핑
    trans_map: dict[str, str] = {}
    cat_map: dict[str, list[str]] = {}
    group_map: dict[str, str] = {}
    importance_map: dict[str, int] = {}
    importance_threshold = curator_cfg.get("importance_threshold", 6)

    for g in curated_groups:
        for idx in g.headline_indices:
            if idx < len(all_new_titles):
                title = all_new_titles[idx]
                trans_map[title] = g.summary
                cat_map[title] = [g.category]
                group_map[title] = g.group_id
                importance_map[title] = g.importance

    logger.info(
        "Curation: %d groups from %d headlines, %d important (threshold=%d)",
        len(curated_groups),
        len(all_new_titles),
        sum(1 for g in curated_groups if g.importance >= importance_threshold),
        importance_threshold,
    )

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
            seen_groups: set[str] = set()
            for g in curated_groups:
                if g.importance < importance_threshold:
                    continue
                if g.group_id in seen_groups:
                    continue
                seen_groups.add(g.group_id)
                lines.append(f"• [{g.importance}/10] [{g.category}] {g.summary}")
            if lines:
                msg = "\n".join(lines)
                slack.send_bot_message(channel=news_channel, message=msg)
            else:
                logger.info("No important headlines to send to Slack")
        except Exception as e:
            logger.error("News channel Slack send failed: %s", e)

    # 2-5. DB 저장
    if news_store is not None:
        from itertools import groupby
        from operator import itemgetter

        seen_groups_db: set[str] = set()
        for g in curated_groups:
            if g.group_id not in seen_groups_db:
                seen_groups_db.add(g.group_id)
                news_store.save_news_group(
                    group_id=g.group_id,
                    summary=g.summary,
                    category=g.category,
                    importance=g.importance,
                    reason=g.reason,
                    symbols=g.symbols if g.symbols else None,
                )

        for source, group in groupby(collected, key=itemgetter("source")):
            items_in_source = list(group)
            src_news_items = [c["news_item"] for c in items_in_source]
            src_titles = [c["cleaned_title"] for c in items_in_source]
            src_translated = [trans_map.get(t, t) for t in src_titles]
            src_scores = [score_map.get(t) for t in src_titles]
            src_categories = [cat_map.get(t, []) for t in src_titles]
            src_embeddings = [embed_map.get(t) for t in src_titles]
            src_group_ids = [group_map.get(t) for t in src_titles]
            _save_news_to_db(
                news_store, src_news_items, src_titles, src_translated,
                source, watchlist, src_scores, src_categories, src_embeddings,
                src_group_ids,
            )
    else:
        for title in all_new_titles:
            extract_symbols(title, watchlist)

    # 2-6. symbol_news_map 구축
    for g in curated_groups:
        for sym in g.symbols:
            for idx in g.headline_indices:
                if idx < len(all_new_titles):
                    title = all_new_titles[idx]
                    score = score_map.get(title)
                    if score is not None:
                        symbol_news_map.setdefault(sym, []).append(
                            NewsAlertItem(title=title, sentiment_score=score)
                        )

    for title in all_new_titles:
        score = score_map.get(title)
        if score is None:
            continue
        related = extract_symbols(title, watchlist)
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
    group_ids: list[str | None] | None = None,
) -> set[str]:
    """수집한 뉴스를 DB에 저장."""
    records = []
    all_symbols: set[str] = set()
    for i, item in enumerate(news_items):
        original = titles[i] if i < len(titles) else item.get("title", "")
        trans = translated[i] if i < len(translated) else original

        related = extract_symbols(original, watchlist)
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
        gid = group_ids[i] if group_ids and i < len(group_ids) else None

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
            group_id=gid,
        ))

    news_store.save_news_batch(records)
    return all_symbols


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
            _, categories, _ = translator.translate_and_categorize(record.title_original)
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
