import backtrader as bt


class StockUpStrategy(bt.Strategy):
    params = (
        ("rsi_period", 14),
        ("rsi_buy", 30),
        ("rsi_sell", 70),
        ("macd_fast", 12),
        ("macd_slow", 26),
        ("macd_signal", 9),
        ("sma_period", 50),
        ("position_pct", 0.1),
    )

    def __init__(self):
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.rsi_period)
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.p.macd_fast,
            period_me2=self.p.macd_slow,
            period_signal=self.p.macd_signal,
        )
        self.sma = bt.indicators.SMA(self.data.close, period=self.p.sma_period)
        self.order = None

    def next(self):
        if self.order:
            return

        if not self.position:
            if self.rsi[0] < self.p.rsi_buy and self.data.close[0] > self.sma[0]:
                size = int(self.broker.getcash() * self.p.position_pct / self.data.close[0])
                if size > 0:
                    self.order = self.buy(size=size)
        else:
            if self.rsi[0] > self.p.rsi_sell or self.macd.macd[0] < self.macd.signal[0]:
                self.order = self.sell(size=self.position.size)

    def notify_order(self, order):
        if order.status in [order.Completed]:
            self.order = None
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order = None
