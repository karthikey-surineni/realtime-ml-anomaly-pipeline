import structlog

from anomaly_pipeline.core.buffer import RollingBuffer
from anomaly_pipeline.schemas import AnomalyFeatures

logger = structlog.get_logger(__name__)


def compute_features(buffer: RollingBuffer, window: int = 20) -> AnomalyFeatures | None:
    """Compute anomaly detection features from the latest kline in the buffer.

    Returns None if the buffer has insufficient data for the rolling window.

    Features:
        price_roc: (current_close - previous_close) / previous_close
        volume_zscore: (current_volume - rolling_mean) / rolling_std
        spread: current_high - current_low
    """
    if buffer.size < window + 1:
        logger.debug("insufficient_data", buffer_size=buffer.size, required=window + 1)
        return None

    stats = buffer.rolling_stats(window=window)
    last = stats.row(-1, named=True)

    volume_std = last["volume_std"]
    if volume_std is None or volume_std == 0:
        volume_zscore = 0.0
    else:
        volume_zscore = (last["volume"] - last["volume_mean"]) / volume_std

    # Price rate of change: use the second-to-last close as the previous
    prev_close = stats.row(-2, named=True)["close"]
    if prev_close == 0:
        price_roc = 0.0
    else:
        price_roc = (last["close"] - prev_close) / prev_close

    spread = last["high"] - last["low"]

    features = AnomalyFeatures(
        price_roc=price_roc,
        volume_zscore=volume_zscore,
        spread=spread,
    )

    logger.debug("features_computed", price_roc=price_roc, volume_zscore=volume_zscore, spread=spread)
    return features
