import os
from datetime import datetime, timedelta

import pytest

from storage.models import NewsRecord
from storage.news_store import NewsStore


@pytest.fixture
def store(tmp_path):
    db_path = str(tmp_path / "test_news.duckdb")
    s = NewsStore(db_path=db_path)
    s.init_schema()
    yield s
    s.close()


def _make_record(title="Apple reports earnings", translated="애플 실적 발표", **kwargs):
    defaults = {
        "title_original": title,
        "title_translated": translated,
        "source": "financialjuice",
        "link": "https://example.com/1",
        "published_at": datetime.now(),
        "collected_at": datetime.now(),
        "sentiment_score": None,
        "related_symbols": ["AAPL"],
    }
    defaults.update(kwargs)
    return NewsRecord(**defaults)


def test_save_and_retrieve(store):
    record = _make_record()
    news_id = store.save_news(record)
    assert news_id is not None
    assert news_id > 0


def test_duplicate_prevention(store):
    record = _make_record()
    first_id = store.save_news(record)
    second_id = store.save_news(record)
    assert first_id is not None
    assert second_id is None  # 중복이므로 None


def test_save_batch(store):
    records = [
        _make_record(title=f"News {i}", translated=f"뉴스 {i}")
        for i in range(5)
    ]
    saved = store.save_news_batch(records)
    assert saved == 5


def test_search_by_keyword(store):
    store.save_news(_make_record(title="Tesla stock surges", translated="테슬라 주가 급등"))
    store.save_news(_make_record(title="Apple earnings report", translated="애플 실적 보고"))
    store.save_news(_make_record(title="Oil prices drop", translated="유가 하락"))

    results = store.search_by_keyword("Tesla")
    assert len(results) == 1
    assert results[0].title_original == "Tesla stock surges"


def test_search_by_keyword_korean(store):
    store.save_news(_make_record(title="Fed rate decision", translated="연준 금리 결정"))
    results = store.search_by_keyword("금리")
    assert len(results) == 1


def test_search_by_symbol(store):
    store.save_news(_make_record(
        title="MSFT cloud growth",
        translated="마이크로소프트 클라우드 성장",
        related_symbols=["MSFT"],
    ))
    store.save_news(_make_record(
        title="Apple M4 chip",
        translated="애플 M4 칩",
        related_symbols=["AAPL"],
    ))

    results = store.search_by_symbol("MSFT")
    assert len(results) == 1
    assert "MSFT" in results[0].related_symbols


def test_update_sentiment(store):
    news_id = store.save_news(_make_record())
    store.update_sentiment(news_id, 0.75)

    results = store.search_by_keyword("Apple")
    assert len(results) == 1
    assert results[0].sentiment_score == pytest.approx(0.75, abs=0.01)


def test_get_negative_news(store):
    store.save_news(_make_record(
        title="Market crash",
        translated="시장 폭락",
        sentiment_score=-0.8,
    ))
    store.save_news(_make_record(
        title="Good earnings",
        translated="좋은 실적",
        sentiment_score=0.6,
    ))

    results = store.get_negative_news(threshold=-0.3)
    assert len(results) == 1
    assert results[0].sentiment_score < -0.3


def test_get_recent_headlines(store):
    store.save_news(_make_record(title="Recent news", translated="최근 뉴스"))
    results = store.get_recent_headlines(hours=1)
    assert len(results) == 1


def test_sentiment_history(store):
    store.save_news(_make_record(
        title="News 1", translated="뉴스 1",
        sentiment_score=0.5, published_at=datetime.now(),
    ))
    store.save_news(_make_record(
        title="News 2", translated="뉴스 2",
        sentiment_score=-0.3, published_at=datetime.now(),
    ))

    history = store.get_sentiment_history(days=1)
    assert len(history) == 1
    assert history[0]["news_count"] == 2
