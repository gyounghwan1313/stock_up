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

                # 확장 펀더멘털 계산
                fcf = self._calc_fcf(ticker)
                operating_margin = info.get("operatingMargins")
                net_margin = info.get("profitMargins")
                fcf_margin = self._calc_fcf_margin(ticker, fcf)
                roic = self._calc_roic(ticker)
                revenue_cagr = self._calc_revenue_cagr(ticker)
                eps_cagr = self._calc_eps_cagr(ticker)
                insider_pct, institutional_pct = self._get_holder_pcts(ticker)

                # 애널리스트 추천 요약
                recommendation = self._get_recommendation(ticker)

                # 어닝스 일정
                next_earnings = self._get_next_earnings(ticker)

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
                    # 확장 밸류에이션
                    forward_pe=info.get("forwardPE"),
                    peg_ratio=info.get("pegRatio"),
                    ev_to_ebitda=info.get("enterpriseToEbitda"),
                    ev_to_revenue=info.get("enterpriseToRevenue"),
                    # 확장 펀더멘털
                    fcf=fcf,
                    operating_margin=operating_margin,
                    net_margin=net_margin,
                    fcf_margin=fcf_margin,
                    roic=roic,
                    revenue_cagr_5y=revenue_cagr,
                    eps_cagr_5y=eps_cagr,
                    insider_pct=insider_pct,
                    institutional_pct=institutional_pct,
                    short_pct_of_float=info.get("shortPercentOfFloat"),
                    # 애널리스트/밸류에이션
                    target_mean_price=info.get("targetMeanPrice"),
                    target_high_price=info.get("targetHighPrice"),
                    target_low_price=info.get("targetLowPrice"),
                    recommendation=recommendation,
                    num_analyst_opinions=info.get("numberOfAnalystOpinions"),
                    next_earnings_date=next_earnings,
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

    def _calc_fcf(self, ticker: yf.Ticker) -> float | None:
        try:
            cf = ticker.cashflow
            if cf is None or cf.empty:
                return None
            op_cf = cf.loc["Operating Cash Flow"].iloc[0] if "Operating Cash Flow" in cf.index else None
            capex = cf.loc["Capital Expenditure"].iloc[0] if "Capital Expenditure" in cf.index else None
            if op_cf is not None and capex is not None:
                return float(op_cf + capex)  # capex is negative
        except Exception:
            pass
        return None

    def _calc_fcf_margin(self, ticker: yf.Ticker, fcf: float | None) -> float | None:
        if fcf is None:
            return None
        try:
            fin = ticker.financials
            if fin is None or fin.empty:
                return None
            revenue = fin.loc["Total Revenue"].iloc[0] if "Total Revenue" in fin.index else None
            if revenue and revenue > 0:
                return float(fcf / revenue)
        except Exception:
            pass
        return None

    def _calc_roic(self, ticker: yf.Ticker) -> float | None:
        try:
            fin = ticker.financials
            bs = ticker.balance_sheet
            if fin is None or fin.empty or bs is None or bs.empty:
                return None
            nopat = None
            if "EBIT" in fin.index:
                ebit = fin.loc["EBIT"].iloc[0]
                tax_rate = 0.21  # US corporate tax rate approximation
                if "Tax Provision" in fin.index and "Pretax Income" in fin.index:
                    tax_prov = fin.loc["Tax Provision"].iloc[0]
                    pretax = fin.loc["Pretax Income"].iloc[0]
                    if pretax and pretax != 0:
                        tax_rate = abs(float(tax_prov / pretax))
                nopat = float(ebit * (1 - tax_rate))

            if nopat is None:
                return None

            total_equity = bs.loc["Stockholders Equity"].iloc[0] if "Stockholders Equity" in bs.index else 0
            total_debt = bs.loc["Total Debt"].iloc[0] if "Total Debt" in bs.index else 0
            invested_capital = float(total_equity + total_debt)
            if invested_capital > 0:
                return float(nopat / invested_capital)
        except Exception:
            pass
        return None

    def _calc_revenue_cagr(self, ticker: yf.Ticker) -> float | None:
        try:
            fin = ticker.financials
            if fin is None or fin.empty or "Total Revenue" not in fin.index:
                return None
            revenues = fin.loc["Total Revenue"].dropna()
            if len(revenues) < 2:
                return None
            latest = float(revenues.iloc[0])
            oldest = float(revenues.iloc[-1])
            years = len(revenues) - 1
            if oldest > 0 and latest > 0 and years > 0:
                return float((latest / oldest) ** (1 / years) - 1)
        except Exception:
            pass
        return None

    def _calc_eps_cagr(self, ticker: yf.Ticker) -> float | None:
        try:
            fin = ticker.financials
            if fin is None or fin.empty:
                return None
            # Use Basic EPS if available, else compute from net income / shares
            info = ticker.info
            shares = info.get("sharesOutstanding")
            if "Net Income" not in fin.index or not shares:
                return None
            net_incomes = fin.loc["Net Income"].dropna()
            if len(net_incomes) < 2:
                return None
            latest_eps = float(net_incomes.iloc[0]) / shares
            oldest_eps = float(net_incomes.iloc[-1]) / shares
            years = len(net_incomes) - 1
            if oldest_eps > 0 and latest_eps > 0 and years > 0:
                return float((latest_eps / oldest_eps) ** (1 / years) - 1)
        except Exception:
            pass
        return None

    def _get_holder_pcts(self, ticker: yf.Ticker) -> tuple[float | None, float | None]:
        insider_pct = None
        institutional_pct = None
        try:
            holders = ticker.major_holders
            if holders is not None and not holders.empty:
                for _, row in holders.iterrows():
                    label = str(row.iloc[1]).lower() if len(row) > 1 else ""
                    val = row.iloc[0]
                    if "insider" in label:
                        insider_pct = self._parse_pct(val)
                    elif "institution" in label:
                        institutional_pct = self._parse_pct(val)
        except Exception:
            pass
        return insider_pct, institutional_pct

    @staticmethod
    def _parse_pct(val) -> float | None:
        try:
            if isinstance(val, str):
                return float(val.replace("%", "")) / 100
            return float(val)
        except (ValueError, TypeError):
            return None

    def _get_recommendation(self, ticker: yf.Ticker) -> str | None:
        try:
            recs = ticker.recommendations
            if recs is not None and not recs.empty:
                latest = recs.iloc[-1]
                for col in ("To Grade", "toGrade", "strongBuy", "buy"):
                    if col in latest.index:
                        return str(latest[col])
            info = ticker.info
            return info.get("recommendationKey")
        except Exception:
            return None

    def _get_next_earnings(self, ticker: yf.Ticker) -> str | None:
        try:
            dates = ticker.earnings_dates
            if dates is not None and not dates.empty:
                from datetime import datetime as dt
                now = dt.now()
                future = dates[dates.index >= pd.Timestamp(now)]
                if not future.empty:
                    return str(future.index[0].date())
                return str(dates.index[0].date())
        except Exception:
            return None
