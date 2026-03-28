import numpy as np
import structlog
from sklearn.ensemble import IsolationForest

from anomaly_pipeline.config import settings
from anomaly_pipeline.core.buffer import RollingBuffer
from anomaly_pipeline.schemas import AnomalyFeatures

logger = structlog.get_logger(__name__)


class AnomalyDetector:
    """IsolationForest wrapper for real-time anomaly detection on kline features."""

    def __init__(self, contamination: float = settings.anomaly_contamination) -> None:
        self._model = IsolationForest(
            contamination=contamination,
            n_estimators=100,
            random_state=42,
        )
        self._is_trained = False

    @property
    def is_trained(self) -> bool:
        return self._is_trained

    def train(self, buffer: RollingBuffer, window: int = 20) -> None:
        """Train the model on historical features from the buffer.

        Requires at least window+1 rows in the buffer.
        """
        if buffer.size < window + 1:
            logger.warning("detector_train_skipped", buffer_size=buffer.size, required=window + 1)
            return

        stats = buffer.rolling_stats(window=window)

        # Drop rows with nulls from the rolling window warm-up period
        close_vals = stats.get_column("close").to_numpy()
        volume_vals = stats.get_column("volume").to_numpy()
        volume_mean = stats.get_column("volume_mean").to_numpy()
        volume_std = stats.get_column("volume_std").to_numpy()
        high_vals = stats.get_column("high").to_numpy()
        low_vals = stats.get_column("low").to_numpy()

        # Build feature matrix starting from index `window` (where rolling stats are valid)
        rows = []
        for i in range(window, len(close_vals)):
            prev_close = close_vals[i - 1]
            price_roc = (close_vals[i] - prev_close) / prev_close if prev_close != 0 else 0.0
            v_std = volume_std[i]
            volume_zscore = (volume_vals[i] - volume_mean[i]) / v_std if v_std and v_std != 0 else 0.0
            spread = high_vals[i] - low_vals[i]
            rows.append([price_roc, volume_zscore, spread])

        if not rows:
            logger.warning("detector_train_no_valid_rows")
            return

        X = np.array(rows)
        self._model.fit(X)
        self._is_trained = True
        logger.info("detector_trained", samples=len(rows))

    def predict(self, features: AnomalyFeatures) -> tuple[bool, float]:
        """Score a single feature vector.

        Returns:
            (is_anomaly, anomaly_score) where score is from decision_function
            (more negative = more anomalous).
        """
        X = np.array([[features.price_roc, features.volume_zscore, features.spread]])
        label = self._model.predict(X)[0]  # 1 = normal, -1 = anomaly
        score = self._model.decision_function(X)[0]

        is_anomaly = label == -1
        if is_anomaly:
            logger.warning(
                "anomaly_detected",
                price_roc=features.price_roc,
                volume_zscore=features.volume_zscore,
                spread=features.spread,
                score=float(score),
            )
        return is_anomaly, float(score)
