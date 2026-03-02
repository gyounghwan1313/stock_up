from typing import Protocol

import pandas as pd

from core.models import FundamentalData, ScreenerResult, StockQuote


class PriceProvider(Protocol):
    def get_current_price(self, symbol: str) -> StockQuote: ...
    def get_historical(self, symbol: str, period: str) -> pd.DataFrame: ...


class FundamentalProvider(Protocol):
    def get_fundamentals(self, symbol: str) -> FundamentalData: ...


class NewsProvider(Protocol):
    def fetch_news(self) -> list[dict]: ...


class ScreenerProvider(Protocol):
    def discover_stocks(self) -> list[ScreenerResult]: ...
