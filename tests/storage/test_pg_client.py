"""PgNewsStore 테스트 — 실제 DB 없이 모듈 import 및 모델 테스트"""
from datetime import datetime

from storage.models import NewsRecord


def test_news_record_creation():
    record = NewsRecord(
        title_original="Apple reports earnings",
        title_translated="애플 실적 발표",
        source="financialjuice",
        link="https://example.com/1",
        published_at=datetime(2025, 1, 15, 10, 0),
        collected_at=datetime.now(),
        sentiment_score=0.5,
        related_symbols=["AAPL"],
    )
    assert record.title_original == "Apple reports earnings"
    assert record.related_symbols == ["AAPL"]
    assert record.sentiment_score == 0.5


def test_news_record_optional_fields():
    record = NewsRecord(
        title_original="Test",
        title_translated="테스트",
        source="test",
        link="",
        published_at=None,
        collected_at=datetime.now(),
    )
    assert record.sentiment_score is None
    assert record.related_symbols is None
    assert record.id is None
