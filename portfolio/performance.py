from portfolio.paper_trader import PaperTrader


def calculate_performance(trader: PaperTrader) -> dict:
    total_trades = len(trader.closed_positions)
    winning = [p for p in trader.closed_positions if p.pnl > 0]
    losing = [p for p in trader.closed_positions if p.pnl < 0]

    win_rate = len(winning) / total_trades * 100 if total_trades > 0 else 0
    total_pnl = sum(p.pnl for p in trader.closed_positions)
    avg_pnl_pct = sum(p.pnl_pct for p in trader.closed_positions) / total_trades if total_trades else 0

    open_positions = [p for p in trader.positions if p.is_open]

    return {
        "total_value": trader.total_value,
        "cash": trader.cash,
        "total_pnl": total_pnl,
        "total_pnl_pct": trader.total_pnl / trader.initial_capital * 100 if trader.initial_capital else 0,
        "total_trades": total_trades,
        "win_rate": win_rate,
        "avg_pnl_pct": avg_pnl_pct,
        "open_positions": len(open_positions),
    }


def format_performance_summary(trader: PaperTrader) -> str:
    perf = calculate_performance(trader)
    lines = [
        ":bar_chart: *Paper Trading Summary*",
        f"Total Value: ${perf['total_value']:,.2f}",
        f"Cash: ${perf['cash']:,.2f}",
        f"Total PnL: ${perf['total_pnl']:,.2f} ({perf['total_pnl_pct']:+.1f}%)",
        f"Trades: {perf['total_trades']} | Win Rate: {perf['win_rate']:.0f}%",
        f"Open Positions: {perf['open_positions']}",
    ]
    return "\n".join(lines)
