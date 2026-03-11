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
    -- 확장 기술적 지표
    ema_12          REAL,
    ema_26          REAL,
    ema_50          REAL,
    obv             REAL,
    golden_cross    BOOLEAN,
    death_cross     BOOLEAN,
    pivot_point     REAL,
    support_level   REAL,
    resistance_level REAL,
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
    -- 확장 펀더멘탈
    forward_pe      REAL,
    peg_ratio       REAL,
    ev_to_ebitda    REAL,
    ev_to_revenue   REAL,
    fcf             REAL,
    operating_margin REAL,
    net_margin      REAL,
    fcf_margin      REAL,
    roic            REAL,
    short_pct       REAL,
    target_mean_price REAL,
    recommendation  VARCHAR,
    -- 메타
    collected_at    TIMESTAMP NOT NULL,
    UNIQUE(symbol, date)
);
"""

MIGRATE_COLUMNS_SQL = [
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS ema_12 REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS ema_26 REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS ema_50 REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS obv REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS golden_cross BOOLEAN",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS death_cross BOOLEAN",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS pivot_point REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS support_level REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS resistance_level REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS forward_pe REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS peg_ratio REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS ev_to_ebitda REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS ev_to_revenue REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS fcf REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS operating_margin REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS net_margin REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS fcf_margin REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS roic REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS short_pct REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS target_mean_price REAL",
    "ALTER TABLE stock_snapshots ADD COLUMN IF NOT EXISTS recommendation VARCHAR",
]

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
        # 기존 테이블에 새 컬럼 추가 (마이그레이션)
        for sql in MIGRATE_COLUMNS_SQL:
            try:
                self.conn.execute(sql)
            except Exception:
                pass  # 이미 존재하거나 지원되지 않으면 무시
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
        snap_date = date.date() if isinstance(date, datetime) else date

        open_ = getattr(quote, "open", None)
        high = getattr(quote, "high", None)
        low = getattr(quote, "low", None)
        close = getattr(quote, "price", None)
        volume = getattr(quote, "volume", None)

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

        # 확장 기술적 지표
        ema = getattr(indicators, "ema", {}) or {}
        ema_12 = ema.get(12)
        ema_26 = ema.get(26)
        ema_50 = ema.get(50)
        obv = getattr(indicators, "obv", None)
        golden_cross = getattr(indicators, "golden_cross", None)
        death_cross = getattr(indicators, "death_cross", None)
        pivot_point = getattr(indicators, "pivot", None)
        support_level = getattr(indicators, "support", None)
        resistance_level = getattr(indicators, "resistance", None)

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

        # 확장 펀더멘털
        forward_pe = getattr(fundamentals, "forward_pe", None)
        peg_ratio = getattr(fundamentals, "peg_ratio", None)
        ev_to_ebitda = getattr(fundamentals, "ev_to_ebitda", None)
        ev_to_revenue = getattr(fundamentals, "ev_to_revenue", None)
        fcf = getattr(fundamentals, "fcf", None)
        operating_margin = getattr(fundamentals, "operating_margin", None)
        net_margin = getattr(fundamentals, "net_margin", None)
        fcf_margin = getattr(fundamentals, "fcf_margin", None)
        roic = getattr(fundamentals, "roic", None)
        short_pct = getattr(fundamentals, "short_pct_of_float", None)
        target_mean_price = getattr(fundamentals, "target_mean_price", None)
        recommendation = getattr(fundamentals, "recommendation", None)

        self.conn.execute(
            """
            INSERT INTO stock_snapshots (
                symbol, date, open, high, low, close, volume,
                rsi, macd, macd_signal, macd_histogram,
                sma_20, sma_50, sma_200,
                bollinger_upper, bollinger_middle, bollinger_lower,
                ema_12, ema_26, ema_50, obv, golden_cross, death_cross,
                pivot_point, support_level, resistance_level,
                per, pbr, psr, roe, eps, dividend_yield, debt_to_equity,
                market_cap, sector, industry,
                forward_pe, peg_ratio, ev_to_ebitda, ev_to_revenue,
                fcf, operating_margin, net_margin, fcf_margin, roic,
                short_pct, target_mean_price, recommendation,
                collected_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?
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
                ema_12 = EXCLUDED.ema_12,
                ema_26 = EXCLUDED.ema_26,
                ema_50 = EXCLUDED.ema_50,
                obv = EXCLUDED.obv,
                golden_cross = EXCLUDED.golden_cross,
                death_cross = EXCLUDED.death_cross,
                pivot_point = EXCLUDED.pivot_point,
                support_level = EXCLUDED.support_level,
                resistance_level = EXCLUDED.resistance_level,
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
                forward_pe = EXCLUDED.forward_pe,
                peg_ratio = EXCLUDED.peg_ratio,
                ev_to_ebitda = EXCLUDED.ev_to_ebitda,
                ev_to_revenue = EXCLUDED.ev_to_revenue,
                fcf = EXCLUDED.fcf,
                operating_margin = EXCLUDED.operating_margin,
                net_margin = EXCLUDED.net_margin,
                fcf_margin = EXCLUDED.fcf_margin,
                roic = EXCLUDED.roic,
                short_pct = EXCLUDED.short_pct,
                target_mean_price = EXCLUDED.target_mean_price,
                recommendation = EXCLUDED.recommendation,
                collected_at = EXCLUDED.collected_at
            """,
            [
                symbol, snap_date, open_, high, low, close, volume,
                rsi, macd, macd_signal, macd_histogram,
                sma_20, sma_50, sma_200,
                bollinger_upper, bollinger_middle, bollinger_lower,
                ema_12, ema_26, ema_50, obv, golden_cross, death_cross,
                pivot_point, support_level, resistance_level,
                per, pbr, psr, roe, eps, dividend_yield, debt_to_equity,
                market_cap, sector, industry,
                forward_pe, peg_ratio, ev_to_ebitda, ev_to_revenue,
                fcf, operating_margin, net_margin, fcf_margin, roic,
                short_pct, target_mean_price, recommendation,
                datetime.now(),
            ],
        )

    def save_snapshots_batch(self, snapshots: list[dict]) -> int:
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
