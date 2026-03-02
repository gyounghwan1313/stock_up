import hashlib
import logging
import os
from datetime import datetime
from typing import Optional

import duckdb

from storage.models import NewsRecord

logger = logging.getLogger(__name__)

from sender.translator import CATEGORIES

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

CATEGORIES_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS categories (
    id   INTEGER PRIMARY KEY DEFAULT nextval('categories_id_seq'),
    name VARCHAR UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS news_categories (
    news_id      INTEGER REFERENCES news(id),
    category_id  INTEGER REFERENCES categories(id),
    PRIMARY KEY (news_id, category_id)
);

CREATE OR REPLACE VIEW news_with_categories AS
SELECT n.*,
       list(c.name ORDER BY c.id) AS categories
FROM news n
LEFT JOIN news_categories nc ON n.id = nc.news_id
LEFT JOIN categories c ON nc.category_id = c.id
GROUP BY n.id, n.title_original, n.title_translated, n.source, n.link,
         n.published_at, n.collected_at, n.sentiment_score, n.related_symbols, n.title_hash;
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
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS categories_id_seq START 1")
        self.conn.execute(CATEGORIES_SCHEMA_SQL)
        # 시드 데이터 삽입
        for name in CATEGORIES:
            self.conn.execute(
                "INSERT INTO categories (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                [name],
            )
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

        news_id = result[0] if result else None
        if news_id and record.categories:
            self._save_news_categories(news_id, record.categories)
        return news_id

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

    def search_by_category(self, category_name: str, days: int = 30, limit: int = 50) -> list[NewsRecord]:
        sql = f"""
            SELECT DISTINCT n.id, n.title_original, n.title_translated, n.source, n.link,
                   n.published_at, n.collected_at, n.sentiment_score, n.related_symbols
            FROM news n
            JOIN news_categories nc ON n.id = nc.news_id
            JOIN categories c ON nc.category_id = c.id
            WHERE c.name = ?
              AND n.collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            ORDER BY n.published_at DESC NULLS LAST
            LIMIT ?
        """
        return self._fetch_records(sql, [category_name, limit])

    def get_all_categories(self) -> list[dict]:
        rows = self.conn.execute("SELECT id, name FROM categories ORDER BY id").fetchall()
        return [{"id": row[0], "name": row[1]} for row in rows]

    def update_categories(self, news_id: int, category_names: list[str]) -> None:
        self.conn.execute("DELETE FROM news_categories WHERE news_id = ?", [news_id])
        self._save_news_categories(news_id, category_names)

    def _save_news_categories(self, news_id: int, category_names: list[str]) -> None:
        for name in category_names:
            cat = self.conn.execute(
                "SELECT id FROM categories WHERE name = ?", [name]
            ).fetchone()
            if cat:
                self.conn.execute(
                    "INSERT INTO news_categories (news_id, category_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    [news_id, cat[0]],
                )

    def _fetch_records(self, sql: str, params: list) -> list[NewsRecord]:
        rows = self.conn.execute(sql, params).fetchall()
        records = []
        for row in rows:
            news_id = row[0]
            # 카테고리 조회
            cat_rows = self.conn.execute(
                """SELECT c.name FROM categories c
                   JOIN news_categories nc ON c.id = nc.category_id
                   WHERE nc.news_id = ?""",
                [news_id],
            ).fetchall()
            categories = [r[0] for r in cat_rows] if cat_rows else None

            records.append(NewsRecord(
                id=news_id,
                title_original=row[1],
                title_translated=row[2],
                source=row[3],
                link=row[4],
                published_at=row[5],
                collected_at=row[6],
                sentiment_score=row[7],
                related_symbols=row[8],
                categories=categories,
            ))
        return records

    def get_uncategorized_news(self, limit: int = 500) -> list[NewsRecord]:
        """카테고리가 없는 뉴스 목록 조회"""
        sql = """
            SELECT n.id, n.title_original, n.title_translated, n.source, n.link,
                   n.published_at, n.collected_at, n.sentiment_score, n.related_symbols
            FROM news n
            LEFT JOIN news_categories nc ON n.id = nc.news_id
            WHERE nc.news_id IS NULL
            ORDER BY n.collected_at DESC
            LIMIT ?
        """
        rows = self.conn.execute(sql, [limit]).fetchall()
        return [
            NewsRecord(
                id=row[0], title_original=row[1], title_translated=row[2],
                source=row[3], link=row[4], published_at=row[5],
                collected_at=row[6], sentiment_score=row[7],
                related_symbols=row[8],
            )
            for row in rows
        ]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
