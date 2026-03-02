import logging

import backtrader as bt
import yfinance as yf

from backtest.strategy import StockUpStrategy

logger = logging.getLogger(__name__)


def run_backtest(
    symbol: str,
    period: str = "2y",
    initial_cash: float = 100000,
    strategy_params: dict | None = None,
) -> dict:
    cerebro = bt.Cerebro()

    # 데이터 가져오기
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=period)
    if df.empty:
        raise ValueError(f"No data for {symbol}")

    df.index = df.index.tz_localize(None)
    data = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data)

    # 전략 추가
    params = strategy_params or {}
    cerebro.addstrategy(StockUpStrategy, **params)

    # 브로커 설정
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=0.001)

    # 분석기 추가
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")

    # 실행
    start_value = cerebro.broker.getvalue()
    results = cerebro.run()
    end_value = cerebro.broker.getvalue()

    strat = results[0]
    sharpe = strat.analyzers.sharpe.get_analysis()
    drawdown = strat.analyzers.drawdown.get_analysis()
    returns = strat.analyzers.returns.get_analysis()
    trades = strat.analyzers.trades.get_analysis()

    total_trades = trades.get("total", {}).get("total", 0)

    return {
        "symbol": symbol,
        "period": period,
        "start_value": start_value,
        "end_value": end_value,
        "total_return_pct": (end_value - start_value) / start_value * 100,
        "sharpe_ratio": sharpe.get("sharperatio"),
        "max_drawdown_pct": drawdown.get("max", {}).get("drawdown", 0),
        "total_trades": total_trades,
    }
