import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from shared.models import FundamentalData, StockQuote

logger = logging.getLogger(__name__)

# ── Price Provider ──

_HISTORICAL_TTL = timedelta(hours=1)
_MAX_RETRIES = 2
_RETRY_DELAY = 3
_TIMEOUT = 30


def _create_ticker(symbol: str) -> yf.Ticker:
    ticker = yf.Ticker(symbol)
    if hasattr(ticker, "session") and ticker.session is not None:
        ticker.session.timeout = _TIMEOUT
    return ticker


def _retry(fn, symbol: str):
    last_err = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if attempt < _MAX_RETRIES:
                logger.warning(
                    "%s: attempt %d failed (%s), retrying in %ds...",
                    symbol, attempt + 1, e, _RETRY_DELAY,
                )
                time.sleep(_RETRY_DELAY)
    raise last_err  # type: ignore[misc]


class YFinancePriceProvider:
    _historical_cache: dict[tuple[str, str], tuple[datetime, pd.DataFrame]] = {}

    def get_current_price(self, symbol: str) -> StockQuote:
        def _fetch():
            ticker = _create_ticker(symbol)
            info = ticker.fast_info
            return StockQuote(
                symbol=symbol,
                price=info.last_price,
                open=info.open,
                high=info.day_high,
                low=info.day_low,
                volume=info.last_volume,
                timestamp=datetime.now(),
            )

        return _retry(_fetch, symbol)

    def get_historical(self, symbol: str, period: str = "6mo") -> pd.DataFrame:
        key = (symbol, period)
        cached = self._historical_cache.get(key)
        if cached is not None:
            cached_at, df = cached
            if datetime.now() - cached_at < _HISTORICAL_TTL:
                logger.debug("Historical cache hit: %s (%s)", symbol, period)
                return df

        def _fetch():
            ticker = _create_ticker(symbol)
            df = ticker.history(period=period)
            if df.empty:
                raise ValueError(f"{symbol}: 과거 데이터를 가져올 수 없습니다")
            return df

        df = _retry(_fetch, symbol)
        self._historical_cache[key] = (datetime.now(), df)
        logger.debug("Historical cache miss: %s (%s), fetched and cached", symbol, period)
        return df


# ── Fundamental Provider ──

class YFinanceFundamentalProvider:
    def get_fundamentals(self, symbol: str) -> FundamentalData:
        last_err = None
        for attempt in range(_MAX_RETRIES + 1):
            try:
                ticker = yf.Ticker(symbol)
                if hasattr(ticker, "session") and ticker.session is not None:
                    ticker.session.timeout = _TIMEOUT
                info = ticker.info

                return FundamentalData(
                    symbol=symbol,
                    per=info.get("trailingPE"),
                    pbr=info.get("priceToBook"),
                    eps=info.get("trailingEps"),
                    market_cap=info.get("marketCap"),
                    dividend_yield=info.get("dividendYield"),
                    sector=info.get("sector"),
                    industry=info.get("industry"),
                    psr=info.get("priceToSalesTrailing12Months"),
                    roe=info.get("returnOnEquity"),
                    debt_to_equity=info.get("debtToEquity"),
                )
            except Exception as e:
                last_err = e
                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "%s: fundamentals attempt %d failed (%s), retrying in %ds...",
                        symbol, attempt + 1, e, _RETRY_DELAY,
                    )
                    time.sleep(_RETRY_DELAY)
        raise last_err  # type: ignore[misc]
