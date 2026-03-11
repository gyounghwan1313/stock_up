import numpy as np
import pandas as pd

from shared.models import IndicatorResult


def compute_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def compute_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff())
    direction.iloc[0] = 0
    return (volume * direction).cumsum()


def detect_cross(sma_short: pd.Series, sma_long: pd.Series) -> tuple[bool, bool]:
    """최근 2일 기준 골든크로스/데스크로스 감지. (golden, death) 반환."""
    if len(sma_short) < 2 or len(sma_long) < 2:
        return False, False
    prev_diff = sma_short.iloc[-2] - sma_long.iloc[-2]
    curr_diff = sma_short.iloc[-1] - sma_long.iloc[-1]
    golden = prev_diff <= 0 and curr_diff > 0
    death = prev_diff >= 0 and curr_diff < 0
    return golden, death


def compute_pivot_points(high: float, low: float, close: float) -> tuple[float, float, float]:
    """피봇 포인트, 지지선(S1), 저항선(R1) 계산."""
    pivot = (high + low + close) / 3
    support = 2 * pivot - high
    resistance = 2 * pivot - low
    return pivot, support, resistance


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def compute_sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period).mean()


def compute_bollinger(
    series: pd.Series, period: int = 20, num_std: float = 2.0
) -> tuple[pd.Series, pd.Series, pd.Series]:
    middle = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    upper = middle + num_std * std
    lower = middle - num_std * std
    return upper, middle, lower


def calculate_indicators(
    df: pd.DataFrame, symbol: str, config: dict
) -> IndicatorResult:
    close = df["Close"]
    ind_cfg = config.get("indicators", {})

    rsi_period = ind_cfg.get("rsi_period", 14)
    rsi_series = compute_rsi(close, rsi_period)
    rsi_val = rsi_series.iloc[-1] if not rsi_series.empty else None

    macd_fast = ind_cfg.get("macd_fast", 12)
    macd_slow = ind_cfg.get("macd_slow", 26)
    macd_sig = ind_cfg.get("macd_signal", 9)
    macd_line, signal_line, histogram = compute_macd(close, macd_fast, macd_slow, macd_sig)

    sma_periods = ind_cfg.get("sma_periods", [20, 50, 200])
    sma_values = {}
    sma_series = {}
    for p in sma_periods:
        s = compute_sma(close, p)
        sma_values[p] = s.iloc[-1] if not s.empty and pd.notna(s.iloc[-1]) else None
        sma_series[p] = s

    bb_period = ind_cfg.get("bollinger_period", 20)
    bb_std = ind_cfg.get("bollinger_std", 2)
    bb_upper, bb_middle, bb_lower = compute_bollinger(close, bb_period, bb_std)

    # EMA
    ema_periods = ind_cfg.get("ema_periods", [12, 26, 50])
    ema_values = {}
    for p in ema_periods:
        e = compute_ema(close, p)
        ema_values[p] = float(e.iloc[-1]) if not e.empty and pd.notna(e.iloc[-1]) else None

    # OBV
    obv_val = None
    if "Volume" in df.columns:
        obv_series = compute_obv(close, df["Volume"])
        obv_val = float(obv_series.iloc[-1]) if not obv_series.empty else None

    # Golden Cross / Death Cross (SMA50 vs SMA200)
    golden_cross = None
    death_cross = None
    if 50 in sma_series and 200 in sma_series:
        s50 = sma_series[50].dropna()
        s200 = sma_series[200].dropna()
        if len(s50) >= 2 and len(s200) >= 2:
            golden_cross, death_cross = detect_cross(s50, s200)

    # 피봇 포인트 (전일 기준)
    pivot_val = support_val = resistance_val = None
    if len(df) >= 2:
        prev = df.iloc[-2]
        pivot_val, support_val, resistance_val = compute_pivot_points(
            float(prev["High"]), float(prev["Low"]), float(prev["Close"])
        )

    return IndicatorResult(
        symbol=symbol,
        rsi=float(rsi_val) if pd.notna(rsi_val) else None,
        macd=float(macd_line.iloc[-1]) if not macd_line.empty else None,
        macd_signal=float(signal_line.iloc[-1]) if not signal_line.empty else None,
        macd_histogram=float(histogram.iloc[-1]) if not histogram.empty else None,
        sma=sma_values,
        bollinger_upper=float(bb_upper.iloc[-1]) if pd.notna(bb_upper.iloc[-1]) else None,
        bollinger_middle=float(bb_middle.iloc[-1]) if pd.notna(bb_middle.iloc[-1]) else None,
        bollinger_lower=float(bb_lower.iloc[-1]) if pd.notna(bb_lower.iloc[-1]) else None,
        ema=ema_values,
        obv=obv_val,
        golden_cross=golden_cross,
        death_cross=death_cross,
        pivot=pivot_val,
        support=support_val,
        resistance=resistance_val,
    )
