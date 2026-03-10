from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Position:
    symbol: str
    entry_price: float
    shares: int
    entry_date: str
    exit_price: Optional[float] = None
    exit_date: Optional[str] = None

    @property
    def is_open(self) -> bool:
        return self.exit_price is None

    @property
    def pnl(self) -> float:
        if self.exit_price is not None:
            return (self.exit_price - self.entry_price) * self.shares
        return 0.0

    @property
    def pnl_pct(self) -> float:
        if self.exit_price is not None and self.entry_price > 0:
            return (self.exit_price - self.entry_price) / self.entry_price * 100
        return 0.0

    def close(self, exit_price: float) -> None:
        self.exit_price = exit_price
        self.exit_date = datetime.now().isoformat()

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "entry_price": self.entry_price,
            "shares": self.shares,
            "entry_date": self.entry_date,
            "exit_price": self.exit_price,
            "exit_date": self.exit_date,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Position":
        return cls(**data)
