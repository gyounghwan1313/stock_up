import json
import os
import tempfile
from datetime import datetime

import pytest

from core.models import IndicatorResult, Signal, SignalType
from portfolio.paper_trader import PaperTrader
from portfolio.position import Position


def _make_signal(symbol="TEST", signal_type=SignalType.BUY, price=100.0):
    return Signal(
        symbol=symbol, signal_type=signal_type, confidence=0.8,
        reasons=["test"], price=price, timestamp=datetime.now(),
    )


@pytest.fixture
def trader(tmp_path):
    config = {
        "initial_capital": 100000,
        "max_position_pct": 0.1,
        "db_path": str(tmp_path / "portfolio.json"),
    }
    return PaperTrader(config)


def test_buy_creates_position(trader):
    signal = _make_signal(price=50.0)
    trader.execute_signal(signal)
    assert len(trader.positions) == 1
    assert trader.positions[0].symbol == "TEST"
    assert trader.cash < 100000


def test_sell_closes_position(trader):
    trader.execute_signal(_make_signal(price=50.0))
    trader.execute_signal(_make_signal(signal_type=SignalType.SELL, price=60.0))
    closed = [p for p in trader.positions if not p.is_open]
    assert len(closed) == 1
    assert closed[0].pnl > 0


def test_no_duplicate_buy(trader):
    trader.execute_signal(_make_signal(price=50.0))
    trader.execute_signal(_make_signal(price=55.0))
    assert len(trader.positions) == 1


def test_save_and_load(trader, tmp_path):
    trader.execute_signal(_make_signal(price=50.0))
    trader.save()

    config = {
        "initial_capital": 100000,
        "max_position_pct": 0.1,
        "db_path": str(tmp_path / "portfolio.json"),
    }
    loaded = PaperTrader(config)
    assert len(loaded.positions) == 1
    assert loaded.cash == trader.cash


class TestPosition:
    def test_open_position(self):
        p = Position(symbol="AAPL", entry_price=150.0, shares=10, entry_date="2024-01-01")
        assert p.is_open
        assert p.pnl == 0.0

    def test_close_position(self):
        p = Position(symbol="AAPL", entry_price=150.0, shares=10, entry_date="2024-01-01")
        p.close(160.0)
        assert not p.is_open
        assert p.pnl == 100.0
        assert p.pnl_pct == pytest.approx(6.666, rel=0.01)

    def test_to_dict_roundtrip(self):
        p = Position(symbol="AAPL", entry_price=150.0, shares=10, entry_date="2024-01-01")
        d = p.to_dict()
        p2 = Position.from_dict(d)
        assert p2.symbol == p.symbol
        assert p2.entry_price == p.entry_price
