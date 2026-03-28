from datetime import datetime

from pydantic import BaseModel, Field


class BinanceKlineData(BaseModel):
    """Inner kline data from the Binance WebSocket message."""

    t: int = Field(..., description="Kline start time (epoch ms)")
    T: int = Field(..., description="Kline close time (epoch ms)")
    s: str = Field(..., description="Symbol")
    i: str = Field(..., description="Interval")
    o: str = Field(..., description="Open price")
    h: str = Field(..., description="High price")
    l: str = Field(..., description="Low price")
    c: str = Field(..., description="Close price")
    v: str = Field(..., description="Base asset volume")
    n: int = Field(..., description="Number of trades")
    x: bool = Field(..., description="Is this kline closed?")
    q: str = Field(..., description="Quote asset volume")
    V: str = Field(..., description="Taker buy base asset volume")
    Q: str = Field(..., description="Taker buy quote asset volume")


class BinanceKlineEvent(BaseModel):
    """Raw WebSocket kline event from Binance."""

    e: str = Field(..., description="Event type")
    E: int = Field(..., description="Event time (epoch ms)")
    s: str = Field(..., description="Symbol")
    k: BinanceKlineData = Field(..., description="Kline data")


class Kline(BaseModel):
    """Cleaned kline data with numeric types."""

    open_time: int = Field(..., description="Kline open time (epoch ms)")
    close_time: int = Field(..., description="Kline close time (epoch ms)")
    open: float = Field(..., description="Open price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Close price")
    volume: float = Field(..., description="Base asset volume")

    @classmethod
    def from_binance_event(cls, event: BinanceKlineEvent) -> "Kline":
        """Convert a raw Binance kline event to a cleaned Kline."""
        k = event.k
        return cls(
            open_time=k.t,
            close_time=k.T,
            open=float(k.o),
            high=float(k.h),
            low=float(k.l),
            close=float(k.c),
            volume=float(k.v),
        )


class AnomalyFeatures(BaseModel):
    """Feature vector computed for anomaly detection."""

    price_roc: float = Field(..., description="Price rate of change")
    volume_zscore: float = Field(..., description="Volume z-score relative to rolling window")
    spread: float = Field(..., description="High-low spread")


class AnomalyEvent(BaseModel):
    """A detected anomaly event to be persisted and alerted on."""

    timestamp: datetime = Field(..., description="Time the anomaly was detected")
    symbol: str = Field(..., description="Trading pair symbol")
    price: float = Field(..., description="Close price at anomaly time")
    volume: float = Field(..., description="Volume at anomaly time")
    features: AnomalyFeatures = Field(..., description="Computed feature values")
    anomaly_score: float = Field(..., description="Anomaly score from the ML model")
