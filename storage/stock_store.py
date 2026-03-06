import logging
import os
from datetime import datetime
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stock_snapshots (
    id              INTEGER PRIMARY KEY DEFAULT nextval('stock_snapshot_id_seq'),
    symbol          VARCHAR NOT NULL,
    date            DATE NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    volume          BIGINT,
    -- 기술적 지표
    rsi             REAL,
    macd            REAL,
    macd_signal     REAL,
    macd_histogram  REAL,
    sma_20          REAL,
    sma_50          REAL,
    sma_200         REAL,
    bollinger_upper REAL,
    bollinger_middle REAL,
    bollinger_lower REAL,
    -- 펀더멘탈
    per             REAL,
    pbr             REAL,
    psr             REAL,
    roe             REAL,
    eps             REAL,
    dividend_yield  REAL,
    debt_to_equity  REAL,
    market_cap      BIGINT,
    sector          VARCHAR,
    industry        VARCHAR,
    -- 메타
    collected_at    TIMESTAMP NOT NULL,
    UNIQUE(symbol, date)
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_snapshot_symbol ON stock_snapshots(symbol);
CREATE INDEX IF NOT EXISTS idx_snapshot_date ON stock_snapshots(date);
CREATE INDEX IF NOT EXISTS idx_snapshot_symbol_date ON stock_snapshots(symbol, date);
"""


class StockStore:
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
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS stock_snapshot_id_seq START 1")
        self.conn.execute(SCHEMA_SQL)
        self.conn.execute(INDEX_SQL)
        logger.info("DuckDB stock_snapshots schema initialized: %s", self.db_path)

    def save_snapshot(
        self,
        symbol: str,
        date: datetime,
        quote=None,
        indicators=None,
        fundamentals=None,
    ) -> None:
        """스냅샷 저장. 같은 (symbol, date)가 있으면 최신 값으로 갱신."""
        snap_date = date.date() if isinstance(date, datetime) else date

        # quote 데이터
        open_ = getattr(quote, "open", None)
        high = getattr(quote, "high", None)
        low = getattr(quote, "low", None)
        close = getattr(quote, "price", None)
        volume = getattr(quote, "volume", None)

        # 기술적 지표
        rsi = getattr(indicators, "rsi", None)
        macd = getattr(indicators, "macd", None)
        macd_signal = getattr(indicators, "macd_signal", None)
        macd_histogram = getattr(indicators, "macd_histogram", None)
        sma = getattr(indicators, "sma", {}) or {}
        sma_20 = sma.get(20)
        sma_50 = sma.get(50)
        sma_200 = sma.get(200)
        bollinger_upper = getattr(indicators, "bollinger_upper", None)
        bollinger_middle = getattr(indicators, "bollinger_middle", None)
        bollinger_lower = getattr(indicators, "bollinger_lower", None)

        # 펀더멘탈
        per = getattr(fundamentals, "per", None)
        pbr = getattr(fundamentals, "pbr", None)
        psr = getattr(fundamentals, "psr", None)
        roe = getattr(fundamentals, "roe", None)
        eps = getattr(fundamentals, "eps", None)
        dividend_yield = getattr(fundamentals, "dividend_yield", None)
        debt_to_equity = getattr(fundamentals, "debt_to_equity", None)
        market_cap = getattr(fundamentals, "market_cap", None)
        if market_cap is not None:
            market_cap = int(market_cap)
        sector = getattr(fundamentals, "sector", None)
        industry = getattr(fundamentals, "industry", None)

        self.conn.execute(
            """
            INSERT INTO stock_snapshots (
                symbol, date, open, high, low, close, volume,
                rsi, macd, macd_signal, macd_histogram,
                sma_20, sma_50, sma_200,
                bollinger_upper, bollinger_middle, bollinger_lower,
                per, pbr, psr, roe, eps, dividend_yield, debt_to_equity,
                market_cap, sector, industry, collected_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?
            )
            ON CONFLICT (symbol, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                rsi = EXCLUDED.rsi,
                macd = EXCLUDED.macd,
                macd_signal = EXCLUDED.macd_signal,
                macd_histogram = EXCLUDED.macd_histogram,
                sma_20 = EXCLUDED.sma_20,
                sma_50 = EXCLUDED.sma_50,
                sma_200 = EXCLUDED.sma_200,
                bollinger_upper = EXCLUDED.bollinger_upper,
                bollinger_middle = EXCLUDED.bollinger_middle,
                bollinger_lower = EXCLUDED.bollinger_lower,
                per = EXCLUDED.per,
                pbr = EXCLUDED.pbr,
                psr = EXCLUDED.psr,
                roe = EXCLUDED.roe,
                eps = EXCLUDED.eps,
                dividend_yield = EXCLUDED.dividend_yield,
                debt_to_equity = EXCLUDED.debt_to_equity,
                market_cap = EXCLUDED.market_cap,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                collected_at = EXCLUDED.collected_at
            """,
            [
                symbol, snap_date, open_, high, low, close, volume,
                rsi, macd, macd_signal, macd_histogram,
                sma_20, sma_50, sma_200,
                bollinger_upper, bollinger_middle, bollinger_lower,
                per, pbr, psr, roe, eps, dividend_yield, debt_to_equity,
                market_cap, sector, industry, datetime.now(),
            ],
        )

    def save_snapshots_batch(self, snapshots: list[dict]) -> int:
        """배치로 스냅샷 저장. 각 dict는 save_snapshot과 동일한 키를 가짐."""
        saved = 0
        self.conn.execute("BEGIN TRANSACTION")
        try:
            for snap in snapshots:
                self.save_snapshot(
                    symbol=snap["symbol"],
                    date=snap["date"],
                    quote=snap.get("quote"),
                    indicators=snap.get("indicators"),
                    fundamentals=snap.get("fundamentals"),
                )
                saved += 1
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        logger.info("Saved %d stock snapshots", saved)
        return saved

    def get_snapshots(self, symbol: str, days: int = 30) -> list[dict]:
        """특정 종목의 최근 N일 스냅샷 조회."""
        sql = f"""
            SELECT symbol, date, open, high, low, close, volume,
                   rsi, macd, macd_signal, macd_histogram,
                   sma_20, sma_50, sma_200,
                   bollinger_upper, bollinger_middle, bollinger_lower,
                   per, pbr, psr, roe, eps, dividend_yield, debt_to_equity,
                   market_cap, sector, industry, collected_at
            FROM stock_snapshots
            WHERE symbol = ?
              AND date >= CURRENT_DATE - INTERVAL '{int(days)}' DAY
            ORDER BY date DESC
        """
        rows = self.conn.execute(sql, [symbol]).fetchall()
        cols = [
            "symbol", "date", "open", "high", "low", "close", "volume",
            "rsi", "macd", "macd_signal", "macd_histogram",
            "sma_20", "sma_50", "sma_200",
            "bollinger_upper", "bollinger_middle", "bollinger_lower",
            "per", "pbr", "psr", "roe", "eps", "dividend_yield", "debt_to_equity",
            "market_cap", "sector", "industry", "collected_at",
        ]
        return [dict(zip(cols, row)) for row in rows]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
