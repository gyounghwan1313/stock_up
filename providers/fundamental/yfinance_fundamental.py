import logging
import time

import yfinance as yf

from core.models import FundamentalData

logger = logging.getLogger(__name__)

_MAX_RETRIES = 2
_RETRY_DELAY = 3
_TIMEOUT = 30


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
