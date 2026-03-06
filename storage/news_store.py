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

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_news_collected_at ON news(collected_at);
CREATE INDEX IF NOT EXISTS idx_news_source ON news(source);
CREATE INDEX IF NOT EXISTS idx_news_published_at ON news(published_at);
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
         n.published_at, n.collected_at, n.sentiment_score, n.related_symbols, n.title_hash,
         n.embedding;
"""


def _interval(n: int, unit: str = "DAY") -> str:
    """안전한 INTERVAL 리터럴 생성 (정수만 허용)"""
    n = int(n)
    unit = unit.upper()
    if unit not in ("DAY", "HOUR", "MINUTE"):
        raise ValueError(f"Invalid interval unit: {unit}")
    return f"INTERVAL '{n}' {unit}"


class NewsStore:
    def __init__(self, db_path: str = "./data/news.duckdb", read_only: bool = False):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._read_only = read_only
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path, read_only=self._read_only)
        return self._conn

    def init_schema(self) -> None:
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS news_id_seq START 1")
        self.conn.execute(SCHEMA_SQL)
        self.conn.execute(INDEX_SQL)
        # 기존 DB 마이그레이션: embedding 컬럼 추가 (VIEW 생성 전에 실행)
        self.conn.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS embedding FLOAT[]")
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
                              related_symbols, title_hash, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                record.embedding,
            ],
        ).fetchone()

        news_id = result[0] if result else None
        if news_id and record.categories:
            self._save_news_categories(news_id, record.categories)
        return news_id

    def save_news_batch(self, records: list[NewsRecord]) -> int:
        """배치로 뉴스 레코드를 저장합니다 (트랜잭션으로 감싸서 I/O 최소화)."""
        saved = 0
        self.conn.execute("BEGIN TRANSACTION")
        try:
            for record in records:
                result = self.save_news(record)
                if result is not None:
                    saved += 1
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        logger.info("Saved %d/%d news records", saved, len(records))
        return saved

    def search_by_keyword(self, keyword: str, days: int = 30, limit: int = 50) -> list[NewsRecord]:
        sql = f"""
            SELECT n.id, n.title_original, n.title_translated, n.source, n.link,
                   n.published_at, n.collected_at, n.sentiment_score, n.related_symbols,
                   list(c.name ORDER BY c.id) AS categories
            FROM news n
            LEFT JOIN news_categories nc ON n.id = nc.news_id
            LEFT JOIN categories c ON nc.category_id = c.id
            WHERE (n.title_original ILIKE ? OR n.title_translated ILIKE ?)
              AND n.collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            GROUP BY n.id, n.title_original, n.title_translated, n.source, n.link,
                     n.published_at, n.collected_at, n.sentiment_score, n.related_symbols
            ORDER BY n.published_at DESC NULLS LAST
            LIMIT ?
        """
        pattern = f"%{keyword}%"
        return self._fetch_records(sql, [pattern, pattern, limit])

    def search_by_symbol(self, symbol: str, days: int = 30, limit: int = 50) -> list[NewsRecord]:
        sql = f"""
            SELECT n.id, n.title_original, n.title_translated, n.source, n.link,
                   n.published_at, n.collected_at, n.sentiment_score, n.related_symbols,
                   list(c.name ORDER BY c.id) AS categories
            FROM news n
            LEFT JOIN news_categories nc ON n.id = nc.news_id
            LEFT JOIN categories c ON nc.category_id = c.id
            WHERE list_contains(n.related_symbols, ?)
              AND n.collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            GROUP BY n.id, n.title_original, n.title_translated, n.source, n.link,
                     n.published_at, n.collected_at, n.sentiment_score, n.related_symbols
            ORDER BY n.published_at DESC NULLS LAST
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
            SELECT n.id, n.title_original, n.title_translated, n.source, n.link,
                   n.published_at, n.collected_at, n.sentiment_score, n.related_symbols,
                   list(c.name ORDER BY c.id) AS categories
            FROM news n
            LEFT JOIN news_categories nc ON n.id = nc.news_id
            LEFT JOIN categories c ON nc.category_id = c.id
            WHERE n.collected_at >= CURRENT_TIMESTAMP - {_interval(hours, 'HOUR')}
            GROUP BY n.id, n.title_original, n.title_translated, n.source, n.link,
                     n.published_at, n.collected_at, n.sentiment_score, n.related_symbols
            ORDER BY n.published_at DESC NULLS LAST
            LIMIT ?
        """
        return self._fetch_records(sql, [limit])

    def get_negative_news(self, threshold: float = -0.3, days: int = 7, limit: int = 50) -> list[NewsRecord]:
        sql = f"""
            SELECT n.id, n.title_original, n.title_translated, n.source, n.link,
                   n.published_at, n.collected_at, n.sentiment_score, n.related_symbols,
                   list(c.name ORDER BY c.id) AS categories
            FROM news n
            LEFT JOIN news_categories nc ON n.id = nc.news_id
            LEFT JOIN categories c ON nc.category_id = c.id
            WHERE n.sentiment_score IS NOT NULL
              AND n.sentiment_score <= ?
              AND n.collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            GROUP BY n.id, n.title_original, n.title_translated, n.source, n.link,
                     n.published_at, n.collected_at, n.sentiment_score, n.related_symbols
            ORDER BY n.sentiment_score ASC
            LIMIT ?
        """
        return self._fetch_records(sql, [threshold, limit])

    def update_sentiment(self, news_id: int, score: float) -> None:
        self.conn.execute("UPDATE news SET sentiment_score = ? WHERE id = ?", [score, news_id])

    def update_embedding(self, news_id: int, vector: list[float]) -> None:
        self.conn.execute("UPDATE news SET embedding = ? WHERE id = ?", [vector, news_id])

    def get_news_for_clustering(self, days: int = 30, limit: int = 5000) -> list[dict]:
        """클러스터링용 — id, 제목, embedding을 함께 반환. embedding이 있는 것만 조회."""
        sql = f"""
            SELECT id, title_original, title_translated, embedding
            FROM news
            WHERE embedding IS NOT NULL
              AND collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            ORDER BY collected_at DESC
            LIMIT ?
        """
        rows = self.conn.execute(sql, [limit]).fetchall()
        return [
            {"id": row[0], "title_original": row[1], "title_translated": row[2], "embedding": list(row[3])}
            for row in rows
        ]

    def get_news_without_embeddings(self, limit: int = 500) -> list[NewsRecord]:
        """embedding이 없는 뉴스 조회 — 소급 적용(backfill)용."""
        sql = """
            SELECT id, title_original, title_translated, source, link,
                   published_at, collected_at, sentiment_score, related_symbols
            FROM news
            WHERE embedding IS NULL
            ORDER BY collected_at DESC
            LIMIT ?
        """
        rows = self.conn.execute(sql, [limit]).fetchall()
        return [
            NewsRecord(
                id=row[0], title_original=row[1], title_translated=row[2],
                source=row[3], link=row[4], published_at=row[5],
                collected_at=row[6], sentiment_score=row[7], related_symbols=row[8],
            )
            for row in rows
        ]

    def update_related_symbols(self, news_id: int, symbols: list[str]) -> None:
        self.conn.execute("UPDATE news SET related_symbols = ? WHERE id = ?", [symbols, news_id])

    def search_by_category(self, category_name: str, days: int = 30, limit: int = 50) -> list[NewsRecord]:
        sql = f"""
            SELECT n.id, n.title_original, n.title_translated, n.source, n.link,
                   n.published_at, n.collected_at, n.sentiment_score, n.related_symbols,
                   list(c2.name ORDER BY c2.id) AS categories
            FROM news n
            JOIN news_categories nc ON n.id = nc.news_id
            JOIN categories c ON nc.category_id = c.id
            LEFT JOIN news_categories nc2 ON n.id = nc2.news_id
            LEFT JOIN categories c2 ON nc2.category_id = c2.id
            WHERE c.name = ?
              AND n.collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            GROUP BY n.id, n.title_original, n.title_translated, n.source, n.link,
                     n.published_at, n.collected_at, n.sentiment_score, n.related_symbols
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
        """쿼리 결과를 NewsRecord 리스트로 변환 (카테고리 JOIN 포함)."""
        rows = self.conn.execute(sql, params).fetchall()
        records = []
        for row in rows:
            # JOIN으로 가져온 categories 컬럼 (인덱스 9)
            raw_categories = row[9] if len(row) > 9 else None
            # DuckDB list()는 NULL 값을 포함할 수 있으므로 필터링
            categories = [c for c in raw_categories if c is not None] if raw_categories else None

            records.append(NewsRecord(
                id=row[0],
                title_original=row[1],
                title_translated=row[2],
                source=row[3],
                link=row[4],
                published_at=row[5],
                collected_at=row[6],
                sentiment_score=row[7],
                related_symbols=row[8],
                categories=categories if categories else None,
            ))
        return records

    def get_category_sentiment_summary(self, days: int = 7) -> list[dict]:
        """카테고리별 감성 점수 집계 (N일간).

        Returns:
            list[dict] with keys:
                category, avg_sentiment, news_count,
                recent_avg (후반부 평균), older_avg (전반부 평균),
                top_headlines: list[tuple[str, float]]
        """
        half = max(1, days // 2)

        # 카테고리별 전체 평균/건수 + 전반/후반 평균
        sql = f"""
            SELECT
                c.name AS category,
                AVG(n.sentiment_score)   AS avg_sentiment,
                COUNT(*)                 AS news_count,
                AVG(CASE WHEN n.collected_at >= CURRENT_TIMESTAMP - {_interval(half)}
                         THEN n.sentiment_score END) AS recent_avg,
                AVG(CASE WHEN n.collected_at <  CURRENT_TIMESTAMP - {_interval(half)}
                              AND n.collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
                         THEN n.sentiment_score END) AS older_avg
            FROM news n
            JOIN news_categories nc ON n.id = nc.news_id
            JOIN categories c ON nc.category_id = c.id
            WHERE n.sentiment_score IS NOT NULL
              AND n.collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
            GROUP BY c.name
            ORDER BY avg_sentiment DESC
        """
        rows = self.conn.execute(sql).fetchall()

        results = []
        for row in rows:
            category = row[0]
            avg_sent = row[1]
            count = row[2]
            recent_avg = row[3]
            older_avg = row[4]

            # 해당 카테고리의 감성점수 상위 헤드라인
            hl_sql = f"""
                SELECT n.title_translated, n.sentiment_score
                FROM news n
                JOIN news_categories nc ON n.id = nc.news_id
                JOIN categories c ON nc.category_id = c.id
                WHERE c.name = ?
                  AND n.sentiment_score IS NOT NULL
                  AND n.collected_at >= CURRENT_TIMESTAMP - {_interval(days)}
                ORDER BY n.sentiment_score DESC
                LIMIT 5
            """
            hl_rows = self.conn.execute(hl_sql, [category]).fetchall()
            top_headlines = [(r[0], r[1]) for r in hl_rows]

            results.append({
                "category": category,
                "avg_sentiment": float(avg_sent) if avg_sent is not None else 0.0,
                "news_count": int(count),
                "recent_avg": float(recent_avg) if recent_avg is not None else None,
                "older_avg": float(older_avg) if older_avg is not None else None,
                "top_headlines": top_headlines,
            })

        return results

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
