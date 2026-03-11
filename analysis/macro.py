"""매크로 시장 환경 데이터 수집 (yfinance 기반)."""

import logging
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from shared.models import MacroData

logger = logging.getLogger(__name__)

_MACRO_TICKERS = {
    "vix": "^VIX",
    "treasury_10y": "^TNX",
    "treasury_2y": "^IRX",
    "dxy": "DX-Y.NYB",
    "wti_oil": "CL=F",
    "gold": "GC=F",
    "sp500": "^GSPC",
}

_CACHE_TTL = timedelta(hours=1)


class MacroDataProvider:
    def __init__(self):
        self._cache: tuple[datetime, MacroData] | None = None

    def get_macro_data(self) -> MacroData:
        if self._cache is not None:
            cached_at, data = self._cache
            if datetime.now() - cached_at < _CACHE_TTL:
                logger.debug("Macro data cache hit")
                return data

        data = self._fetch()
        self._cache = (datetime.now(), data)
        return data

    def _fetch(self) -> MacroData:
        values: dict[str, float | None] = {}

        for key, ticker_symbol in _MACRO_TICKERS.items():
            try:
                ticker = yf.Ticker(ticker_symbol)
                hist = ticker.history(period="1d")
                if not hist.empty:
                    values[key] = float(hist["Close"].iloc[-1])
                else:
                    values[key] = None
                    logger.warning("No data for macro ticker %s (%s)", key, ticker_symbol)
            except Exception as e:
                values[key] = None
                logger.warning("Failed to fetch macro ticker %s (%s): %s", key, ticker_symbol, e)

        # S&P500 200일 SMA 계산
        sp500_sma200 = None
        sp500_above_sma200 = None
        try:
            sp500_hist = yf.Ticker("^GSPC").history(period="1y")
            if len(sp500_hist) >= 200:
                sp500_sma200 = float(sp500_hist["Close"].rolling(200).mean().iloc[-1])
                if values.get("sp500") is not None and sp500_sma200 is not None:
                    sp500_above_sma200 = values["sp500"] > sp500_sma200
        except Exception as e:
            logger.warning("Failed to compute S&P500 SMA200: %s", e)

        # 10Y-2Y 스프레드 계산
        yield_spread = None
        t10y = values.get("treasury_10y")
        t2y = values.get("treasury_2y")
        if t10y is not None and t2y is not None:
            yield_spread = t10y - t2y

        return MacroData(
            vix=values.get("vix"),
            treasury_10y=t10y,
            treasury_2y=t2y,
            yield_spread_10y_2y=yield_spread,
            dxy=values.get("dxy"),
            wti_oil=values.get("wti_oil"),
            gold=values.get("gold"),
            sp500=values.get("sp500"),
            sp500_sma200=sp500_sma200,
            sp500_above_sma200=sp500_above_sma200,
            timestamp=datetime.now(),
        )
