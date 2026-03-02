import json
import logging
import os
from datetime import datetime

from core.models import Signal, SignalType
from portfolio.position import Position

logger = logging.getLogger(__name__)


class PaperTrader:
    def __init__(self, config: dict):
        self.initial_capital = config.get("initial_capital", 100000)
        self.max_position_pct = config.get("max_position_pct", 0.1)
        self.db_path = config.get("db_path", "./data/portfolio.json")
        self.cash = self.initial_capital
        self.positions: list[Position] = []
        self.closed_positions: list[Position] = []
        self._load()

    def execute_signal(self, signal: Signal) -> None:
        if signal.signal_type == SignalType.BUY:
            self._buy(signal.symbol, signal.price)
        elif signal.signal_type == SignalType.SELL:
            self._sell(signal.symbol, signal.price)

    def _buy(self, symbol: str, price: float) -> None:
        # 이미 포지션이 있으면 스킵
        if any(p.symbol == symbol and p.is_open for p in self.positions):
            logger.info("Already holding %s, skip buy", symbol)
            return

        max_amount = self.cash * self.max_position_pct
        shares = int(max_amount / price)
        if shares <= 0:
            logger.warning("Not enough cash to buy %s", symbol)
            return

        cost = shares * price
        self.cash -= cost
        position = Position(
            symbol=symbol,
            entry_price=price,
            shares=shares,
            entry_date=datetime.now().isoformat(),
        )
        self.positions.append(position)
        logger.info("BUY %d shares of %s @ $%.2f (cost=$%.2f)", shares, symbol, price, cost)

    def _sell(self, symbol: str, price: float) -> None:
        for pos in self.positions:
            if pos.symbol == symbol and pos.is_open:
                pos.close(price)
                proceeds = pos.shares * price
                self.cash += proceeds
                self.closed_positions.append(pos)
                logger.info(
                    "SELL %d shares of %s @ $%.2f (PnL=$%.2f, %.1f%%)",
                    pos.shares, symbol, price, pos.pnl, pos.pnl_pct,
                )
                return
        logger.info("No open position for %s to sell", symbol)

    @property
    def total_value(self) -> float:
        open_value = sum(p.entry_price * p.shares for p in self.positions if p.is_open)
        return self.cash + open_value

    @property
    def total_pnl(self) -> float:
        return self.total_value - self.initial_capital

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        data = {
            "cash": self.cash,
            "initial_capital": self.initial_capital,
            "positions": [p.to_dict() for p in self.positions],
            "closed_positions": [p.to_dict() for p in self.closed_positions],
        }
        with open(self.db_path, "w") as f:
            json.dump(data, f, indent=2)

    def _load(self) -> None:
        if not os.path.exists(self.db_path):
            return
        try:
            with open(self.db_path) as f:
                data = json.load(f)
            self.cash = data.get("cash", self.initial_capital)
            self.positions = [Position.from_dict(p) for p in data.get("positions", [])]
            self.closed_positions = [Position.from_dict(p) for p in data.get("closed_positions", [])]
        except Exception as e:
            logger.error("Failed to load portfolio: %s", e)
