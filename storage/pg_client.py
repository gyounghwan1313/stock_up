import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from storage.models import NewsRecord

load_dotenv()
logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS news (
    id              SERIAL PRIMARY KEY,
    title_original  TEXT NOT NULL,
    title_translated TEXT NOT NULL,
    source          VARCHAR(100) NOT NULL,
    link            TEXT,
    published_at    TIMESTAMPTZ,
    collected_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sentiment_score REAL,
    related_symbols TEXT[],
    title_hash      VARCHAR(64) NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_news_published_at ON news (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_source ON news (source);
CREATE INDEX IF NOT EXISTS idx_news_symbols ON news USING GIN (related_symbols);
CREATE INDEX IF NOT EXISTS idx_news_sentiment ON news (sentiment_score);

-- Full-text search index (영문 + 번역 모두 검색)
CREATE INDEX IF NOT EXISTS idx_news_fts ON news
    USING GIN (to_tsvector('simple', title_original || ' ' || title_translated));
"""


class PgNewsStore:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.getenv("DATABASE_URL", "postgresql://localhost:5432/stock_up")
        self._conn: Optional[psycopg2.extensions.connection] = None

    @property
    def conn(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.autocommit = True
        return self._conn

    def init_schema(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        logger.info("Database schema initialized")

    def save_news(self, record: NewsRecord) -> Optional[int]:
        import hashlib

        title_hash = hashlib.sha256(record.title_original.encode("utf-8")).hexdigest()
        sql = """
            INSERT INTO news (title_original, title_translated, source, link,
                              published_at, collected_at, sentiment_score,
                              related_symbols, title_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (title_hash) DO NOTHING
            RETURNING id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                record.title_original,
                record.title_translated,
                record.source,
                record.link,
                record.published_at,
                record.collected_at,
                record.sentiment_score,
                record.related_symbols,
                title_hash,
            ))
            row = cur.fetchone()
            return row[0] if row else None

    def save_news_batch(self, records: list[NewsRecord]) -> int:
        saved = 0
        for record in records:
            result = self.save_news(record)
            if result is not None:
                saved += 1
        logger.info("Saved %d/%d news records", saved, len(records))
        return saved

    def search_by_keyword(self, keyword: str, days: int = 30, limit: int = 50) -> list[NewsRecord]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        sql = """
            SELECT id, title_original, title_translated, source, link,
                   published_at, collected_at, sentiment_score, related_symbols
            FROM news
            WHERE to_tsvector('simple', title_original || ' ' || title_translated)
                  @@ plainto_tsquery('simple', %s)
              AND collected_at >= %s
            ORDER BY published_at DESC
            LIMIT %s
        """
        return self._fetch_records(sql, (keyword, cutoff, limit))

    def search_by_symbol(self, symbol: str, days: int = 30, limit: int = 50) -> list[NewsRecord]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        sql = """
            SELECT id, title_original, title_translated, source, link,
                   published_at, collected_at, sentiment_score, related_symbols
            FROM news
            WHERE %s = ANY(related_symbols)
              AND collected_at >= %s
            ORDER BY published_at DESC
            LIMIT %s
        """
        return self._fetch_records(sql, (symbol, cutoff, limit))

    def get_sentiment_history(self, days: int = 30) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        sql = """
            SELECT DATE(published_at) as date,
                   AVG(sentiment_score) as avg_sentiment,
                   COUNT(*) as news_count
            FROM news
            WHERE sentiment_score IS NOT NULL
              AND published_at >= %s
            GROUP BY DATE(published_at)
            ORDER BY date DESC
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, (cutoff,))
            return [dict(row) for row in cur.fetchall()]

    def get_recent_headlines(self, hours: int = 24, limit: int = 100) -> list[NewsRecord]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        sql = """
            SELECT id, title_original, title_translated, source, link,
                   published_at, collected_at, sentiment_score, related_symbols
            FROM news
            WHERE collected_at >= %s
            ORDER BY published_at DESC
            LIMIT %s
        """
        return self._fetch_records(sql, (cutoff, limit))

    def get_negative_news(self, threshold: float = -0.3, days: int = 7, limit: int = 50) -> list[NewsRecord]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        sql = """
            SELECT id, title_original, title_translated, source, link,
                   published_at, collected_at, sentiment_score, related_symbols
            FROM news
            WHERE sentiment_score IS NOT NULL
              AND sentiment_score <= %s
              AND collected_at >= %s
            ORDER BY sentiment_score ASC
            LIMIT %s
        """
        return self._fetch_records(sql, (threshold, cutoff, limit))

    def update_sentiment(self, news_id: int, score: float) -> None:
        sql = "UPDATE news SET sentiment_score = %s WHERE id = %s"
        with self.conn.cursor() as cur:
            cur.execute(sql, (score, news_id))

    def update_related_symbols(self, news_id: int, symbols: list[str]) -> None:
        sql = "UPDATE news SET related_symbols = %s WHERE id = %s"
        with self.conn.cursor() as cur:
            cur.execute(sql, (symbols, news_id))

    def _fetch_records(self, sql: str, params: tuple) -> list[NewsRecord]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, params)
            return [
                NewsRecord(
                    id=row["id"],
                    title_original=row["title_original"],
                    title_translated=row["title_translated"],
                    source=row["source"],
                    link=row["link"],
                    published_at=row["published_at"],
                    collected_at=row["collected_at"],
                    sentiment_score=row["sentiment_score"],
                    related_symbols=row["related_symbols"],
                )
                for row in cur.fetchall()
            ]

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
