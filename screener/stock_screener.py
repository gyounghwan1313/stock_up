import io
import logging

import pandas as pd
import requests
import yfinance as yf

from core.models import ScreenerResult

logger = logging.getLogger(__name__)

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100#Components"

_HEADERS = {"User-Agent": "StockUp/1.0 (stock screener)"}


def _fetch_html(url: str) -> str:
    resp = requests.get(url, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def _normalize_symbol(symbol: str) -> str:
    """Wikipedia 심볼 표기(BRK.B)를 yfinance 형식(BRK-B)으로 변환."""
    return symbol.replace(".", "-")


def _fetch_sp500_symbols() -> list[dict]:
    html = _fetch_html(SP500_URL)
    tables = pd.read_html(io.StringIO(html))
    df = tables[0]
    return [
        {
            "symbol": _normalize_symbol(row["Symbol"]),
            "name": row["Security"],
            "sector": row.get("GICS Sector", ""),
        }
        for _, row in df.iterrows()
    ]


def _fetch_nasdaq100_symbols() -> list[dict]:
    html = _fetch_html(NASDAQ100_URL)
    tables = pd.read_html(io.StringIO(html))
    for table in tables:
        if "Ticker" in table.columns or "Symbol" in table.columns:
            col = "Ticker" if "Ticker" in table.columns else "Symbol"
            name_col = "Company" if "Company" in table.columns else col
            return [
                {
                    "symbol": _normalize_symbol(row[col]),
                    "name": row.get(name_col, row[col]),
                    "sector": "",
                }
                for _, row in table.iterrows()
            ]
    return []


UNIVERSE_FETCHERS = {
    "sp500": _fetch_sp500_symbols,
    "nasdaq100": _fetch_nasdaq100_symbols,
}


class StockScreener:
    def __init__(self, discovery_config: dict):
        self.universes: list[str] = discovery_config.get("universes", ["sp500"])
        self.max_candidates: int = discovery_config.get("max_candidates", 20)
        self.filters: dict = discovery_config.get("filters", {})

    def discover_stocks(self, extra_symbols: list[str] | None = None) -> list[ScreenerResult]:
        all_symbols = self._load_universe()

        # 유니버스에 없는 watchlist 종목 추가
        if extra_symbols:
            existing = {s["symbol"] for s in all_symbols}
            for sym in extra_symbols:
                if sym not in existing:
                    all_symbols.append({"symbol": sym, "name": sym, "sector": ""})

        logger.info("Universe loaded: %d symbols", len(all_symbols))
        candidates = self._screen(all_symbols)
        logger.info("Screening complete: %d candidates", len(candidates))
        return candidates[: self.max_candidates]

    def _load_universe(self) -> list[dict]:
        symbols: list[dict] = []
        seen = set()
        for universe in self.universes:
            fetcher = UNIVERSE_FETCHERS.get(universe)
            if not fetcher:
                logger.warning("Unknown universe: %s", universe)
                continue
            for item in fetcher():
                if item["symbol"] not in seen:
                    seen.add(item["symbol"])
                    symbols.append(item)
        return symbols

    def _screen(self, symbols: list[dict]) -> list[ScreenerResult]:
        from indicators.technical import compute_rsi

        min_market_cap = self.filters.get("min_market_cap", 0)
        min_avg_volume = self.filters.get("min_avg_volume", 0)
        max_per = self.filters.get("max_per")
        max_rsi = self.filters.get("max_rsi")

        sym_list = [s["symbol"] for s in symbols]
        name_map = {s["symbol"]: s for s in symbols}

        # Step 1: 전 종목 1개월 OHLCV를 한 번의 요청으로 수집
        logger.info("Batch downloading 1mo data for %d symbols...", len(sym_list))
        raw = yf.download(
            sym_list,
            period="1mo",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
        )

        def get_df(sym: str) -> pd.DataFrame | None:
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if sym not in raw.columns.get_level_values(0):
                        return None
                    df = raw[sym].dropna(how="all")
                else:
                    # 단일 종목일 때 MultiIndex가 아님
                    df = raw.dropna(how="all")
                return df if not df.empty else None
            except Exception:
                return None

        # Step 2: 다운로드 데이터로 거래량 + RSI 필터 (추가 요청 없음)
        pre_filtered: list[tuple[str, pd.DataFrame, float | None]] = []
        for sym in sym_list:
            try:
                df = get_df(sym)
                if df is None:
                    continue

                avg_vol = df["Volume"].mean()
                if avg_vol < min_avg_volume:
                    continue

                rsi_val = None
                if max_rsi:
                    rsi_series = compute_rsi(df["Close"], period=14)
                    rsi_val = float(rsi_series.iloc[-1]) if not rsi_series.empty else None
                    if rsi_val is None or pd.isna(rsi_val) or rsi_val > max_rsi:
                        continue

                pre_filtered.append((sym, df, rsi_val))
            except Exception as e:
                logger.debug("Pre-filter skip %s: %s", sym, e)

        logger.info(
            "Pre-filter: %d/%d symbols passed volume/RSI check",
            len(pre_filtered),
            len(sym_list),
        )

        # Step 3: 사전 필터 통과 종목에만 ticker.info 호출 (market cap, PE)
        results: list[ScreenerResult] = []
        for sym, df, rsi_val in pre_filtered:
            try:
                info = yf.Ticker(sym).info
                market_cap = info.get("marketCap", 0) or 0
                trailing_pe = info.get("trailingPE")
                price = float(df["Close"].iloc[-1])
                avg_volume = float(df["Volume"].mean())
                change_pct = (
                    float((df["Close"].iloc[-1] - df["Close"].iloc[0]) / df["Close"].iloc[0])
                    if len(df) > 1
                    else None
                )

                if market_cap < min_market_cap:
                    continue
                if max_per and trailing_pe and trailing_pe > max_per:
                    continue

                reasons = []
                if trailing_pe and trailing_pe < 15:
                    reasons.append(f"Low PER ({trailing_pe:.1f})")
                if change_pct and change_pct < -0.1:
                    reasons.append(f"1mo drop ({change_pct:.1%})")
                if avg_volume > 2_000_000:
                    reasons.append("High volume")
                if rsi_val is not None and pd.notna(rsi_val):
                    reasons.append(f"RSI={rsi_val:.1f}")

                meta = name_map.get(sym, {})
                results.append(
                    ScreenerResult(
                        symbol=sym,
                        name=info.get("longName", meta.get("name", sym)),
                        sector=info.get("sector", meta.get("sector", "")),
                        market_cap=market_cap,
                        volume=int(avg_volume),
                        price=price,
                        change_pct=change_pct,
                        discovery_reason="; ".join(reasons) if reasons else "Passed filters",
                    )
                )
            except Exception as e:
                logger.debug("Info fetch skip %s: %s", sym, e)

        results.sort(key=lambda r: (r.market_cap or 0), reverse=True)
        return results
