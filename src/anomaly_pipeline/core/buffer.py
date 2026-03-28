import polars as pl
import structlog

from anomaly_pipeline.config import settings
from anomaly_pipeline.schemas import Kline

logger = structlog.get_logger(__name__)


class RollingBuffer:
    """Fixed-size rolling window of klines backed by a Polars DataFrame.

    Maintains the most recent `max_size` klines and provides efficient
    rolling statistics computation.
    """

    def __init__(self, max_size: int = settings.buffer_size) -> None:
        self._max_size = max_size
        self._df = pl.DataFrame(
            schema={
                "open_time": pl.Int64,
                "close_time": pl.Int64,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
            }
        )

    @property
    def size(self) -> int:
        return self._df.height

    @property
    def df(self) -> pl.DataFrame:
        return self._df

    def load(self, klines: list[Kline]) -> None:
        """Bulk-load klines into the buffer (used during bootstrap)."""
        rows = [k.model_dump() for k in klines]
        self._df = pl.DataFrame(rows, schema=self._df.schema)
        self._trim()
        logger.info("buffer_loaded", size=self.size)

    def append(self, kline: Kline) -> None:
        """Append a single kline and drop the oldest if at capacity."""
        row = pl.DataFrame([kline.model_dump()], schema=self._df.schema)
        self._df = pl.concat([self._df, row])
        self._trim()

    def _trim(self) -> None:
        if self._df.height > self._max_size:
            self._df = self._df.tail(self._max_size)

    def rolling_stats(self, window: int = 20) -> pl.DataFrame:
        """Compute rolling mean and std for close price and volume.

        Returns a DataFrame with columns:
            close, volume, close_mean, close_std, volume_mean, volume_std
        """
        return self._df.select(
            "close",
            "volume",
            "high",
            "low",
            pl.col("close").rolling_mean(window_size=window).alias("close_mean"),
            pl.col("close").rolling_std(window_size=window).alias("close_std"),
            pl.col("volume").rolling_mean(window_size=window).alias("volume_mean"),
            pl.col("volume").rolling_std(window_size=window).alias("volume_std"),
        )
