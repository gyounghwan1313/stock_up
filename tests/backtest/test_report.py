from backtest.report import format_backtest_report


def test_format_report():
    result = {
        "symbol": "AAPL",
        "period": "2y",
        "start_value": 100000,
        "end_value": 115000,
        "total_return_pct": 15.0,
        "sharpe_ratio": 1.2,
        "max_drawdown_pct": 8.5,
        "total_trades": 12,
    }
    report = format_backtest_report(result)
    assert "AAPL" in report
    assert "15.00%" in report
    assert "115,000.00" in report
