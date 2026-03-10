def format_backtest_report(result: dict) -> str:
    lines = [
        f"=== Backtest Report: {result['symbol']} ({result['period']}) ===",
        f"Start Value:     ${result['start_value']:>12,.2f}",
        f"End Value:       ${result['end_value']:>12,.2f}",
        f"Total Return:    {result['total_return_pct']:>11.2f}%",
        f"Sharpe Ratio:    {result['sharpe_ratio'] or 'N/A':>12}",
        f"Max Drawdown:    {result['max_drawdown_pct']:>11.2f}%",
        f"Total Trades:    {result['total_trades']:>12}",
    ]
    return "\n".join(lines)
