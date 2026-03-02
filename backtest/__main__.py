import argparse
import sys

from backtest.report import format_backtest_report
from backtest.runner import run_backtest


def main():
    parser = argparse.ArgumentParser(description="Stock Up Backtester")
    parser.add_argument("--symbol", required=True, help="Stock symbol (e.g. AAPL)")
    parser.add_argument("--period", default="2y", help="Data period (default: 2y)")
    parser.add_argument("--cash", type=float, default=100000, help="Initial cash")
    args = parser.parse_args()

    result = run_backtest(symbol=args.symbol, period=args.period, initial_cash=args.cash)
    print(format_backtest_report(result))


if __name__ == "__main__":
    main()
