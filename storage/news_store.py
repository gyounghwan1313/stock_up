import hashlib
import logging
import os
from datetime import datetime
from typing import Optional

import duckdb

from storage.models import NewsRecord

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS news (
    id               INTEGER PRIMARY KEY DEFAULT nextval('news_id_seq'),
    title_original   TEXT NOT NULL,
    title_translated TEXT NOT NULL,
    source           VARCHAR NOT NULL,
    link             TEXT,
    published_at     TIMESTAMP,
    collected_at     TIMESTAMP NOT NULL,
    sentiment_score  REAL,
    related_symbols  VARCHAR[],
    title_hash       VARCHAR(64) NOT NULL UNIQUE
);
"""


def _interval(n: int, unit: str = "DAY") -> str:
    """안전한 INTERVAL 리터럴 생성 (정수만 허용)"""
    n = int(n)
    unit = unit.upper()
    if unit not in ("DAY", "HOUR", "MINUTE"):
        raise ValueError(f"Invalid interval unit: {unit}")
    return f"INTERVAL '{n}' {unit}"


class NewsStore:
    def __init__(self, db_path: str = "./data/news.duckdb"):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
        return self._conn

    def init_schema(self) -> None:
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS news_id_seq START 1")
        self.conn.execute(SCHEMA_SQL)
        logger.info("DuckDB schema initialized: %s", self.db_path)

    def save_news(self, record: NewsRecord) -> Optional[int]:
        title_hash = hashlib.sha256(record.title_original.encode("utf-8")).hexdigest()

        existing = self.conn.execute(
            "SELECT id FROM news WHERE title_hash = ?", [title_hash]
        ).fetchone()
        if existing:
            return None

        result = self.conn.execute(
            """
            INSERT INTO news (title_original, title_translated, source, link,
                              published_at, collected_at, sentiment_score,
                              related_symbols, title_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            [
                record.title_original,
                record.title_translated,
                record.source,
                record.link,
                record.published_at,
                record.collected_at,
                record.sentiment_score,
                record.related_symbols,
                title_hash,
            ],
        ).fetchone()
        return result[0] if result else None

    def save_news_batch(self, records: list[NewsRecord]) -> int:
        saved = 0
        for record in records:
            result = self.save_news(record)
            if result is not None:
                saved += 1
        logger.info("Saved %d/%d news records", saved, len(records))
        return saved

    def search_by_keyword(self, keyword: str, days: int = 30, limit: int = 50) -> list[NewsRecord]:
        sql = f"""
            SELECT id, title_original, title_translated, source, link,
                   published_at, collected_at, sentiment_score, related_symbols
            FROM news
            WHERE (title_original ILIKE ? OR title_translated ILIKE ?)
              AND collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            ORDER BY published_at DESC NULLS LAST
            LIMIT ?
        """
        pattern = f"%{keyword}%"
        return self._fetch_records(sql, [pattern, pattern, limit])

    def search_by_symbol(self, symbol: str, days: int = 30, limit: int = 50) -> list[NewsRecord]:
        sql = f"""
            SELECT id, title_original, title_translated, source, link,
                   published_at, collected_at, sentiment_score, related_symbols
            FROM news
            WHERE list_contains(related_symbols, ?)
              AND collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            ORDER BY published_at DESC NULLS LAST
            LIMIT ?
        """
        return self._fetch_records(sql, [symbol, limit])

    def get_sentiment_history(self, days: int = 30) -> list[dict]:
        sql = f"""
            SELECT CAST(published_at AS DATE) as date,
                   AVG(sentiment_score) as avg_sentiment,
                   COUNT(*) as news_count
            FROM news
            WHERE sentiment_score IS NOT NULL
              AND published_at >= CURRENT_TIMESTAMP - {_interval(days)}
            GROUP BY CAST(published_at AS DATE)
            ORDER BY date DESC
        """
        rows = self.conn.execute(sql).fetchall()
        cols = ["date", "avg_sentiment", "news_count"]
        return [dict(zip(cols, row)) for row in rows]

    def get_recent_headlines(self, hours: int = 24, limit: int = 100) -> list[NewsRecord]:
        sql = f"""
            SELECT id, title_original, title_translated, source, link,
                   published_at, collected_at, sentiment_score, related_symbols
            FROM news
            WHERE collected_at >= CURRENT_TIMESTAMP - {_interval(hours, 'HOUR')}
            ORDER BY published_at DESC NULLS LAST
            LIMIT ?
        """
        return self._fetch_records(sql, [limit])

    def get_negative_news(self, threshold: float = -0.3, days: int = 7, limit: int = 50) -> list[NewsRecord]:
        sql = f"""
            SELECT id, title_original, title_translated, source, link,
                   published_at, collected_at, sentiment_score, related_symbols
            FROM news
            WHERE sentiment_score IS NOT NULL
              AND sentiment_score <= ?
              AND collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            ORDER BY sentiment_score ASC
            LIMIT ?
        """
        return self._fetch_records(sql, [threshold, limit])

    def update_sentiment(self, news_id: int, score: float) -> None:
        self.conn.execute("UPDATE news SET sentiment_score = ? WHERE id = ?", [score, news_id])

    def update_related_symbols(self, news_id: int, symbols: list[str]) -> None:
        self.conn.execute("UPDATE news SET related_symbols = ? WHERE id = ?", [symbols, news_id])

    def _fetch_records(self, sql: str, params: list) -> list[NewsRecord]:
        rows = self.conn.execute(sql, params).fetchall()
        return [
            NewsRecord(
                id=row[0],
                title_original=row[1],
                title_translated=row[2],
                source=row[3],
                link=row[4],
                published_at=row[5],
                collected_at=row[6],
                sentiment_score=row[7],
                related_symbols=row[8],
            )
            for row in rows
        ]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
