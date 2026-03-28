import asyncio
import signal
from datetime import UTC, datetime

import structlog

from anomaly_pipeline.config import settings
from anomaly_pipeline.connections.binance_rest import fetch_historical_klines
from anomaly_pipeline.connections.binance_ws import BinanceWebSocket
from anomaly_pipeline.core.buffer import RollingBuffer
from anomaly_pipeline.core.features import compute_features
from anomaly_pipeline.logger import setup_logging
from anomaly_pipeline.ml.detector import AnomalyDetector
from anomaly_pipeline.schemas import AnomalyEvent
from anomaly_pipeline.storage.sqlite_writer import init_db, write_anomaly


def main() -> None:
    """Entry point for the anomaly-pipeline CLI command."""
    setup_logging(log_level=settings.log_level)
    logger = structlog.get_logger(__name__)
    logger.info("anomaly_pipeline_starting", symbol=settings.symbol, interval=settings.kline_interval)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("anomaly_pipeline_stopped", reason="keyboard_interrupt")


async def _run() -> None:
    """Main async event loop: bootstrap, train, stream, detect."""
    logger = structlog.get_logger(__name__)
    loop = asyncio.get_running_loop()

    shutdown_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    # Initialize storage
    await init_db()

    # Bootstrap: fetch historical klines and populate buffer
    buffer = RollingBuffer()
    klines = await fetch_historical_klines()
    if not klines:
        logger.error("bootstrap_failed", msg="No historical data available. Exiting.")
        return
    buffer.load(klines)

    # Train the anomaly detector on historical features
    detector = AnomalyDetector()
    detector.train(buffer)

    if not detector.is_trained:
        logger.error("detector_training_failed", msg="Could not train model. Exiting.")
        return

    # Stream live klines and run inference
    ws = BinanceWebSocket()
    consumer_task = asyncio.create_task(_consume(ws, buffer, detector, shutdown_event))

    await shutdown_event.wait()
    consumer_task.cancel()

    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    logger.info("anomaly_pipeline_shutdown_complete")


async def _consume(
    ws: BinanceWebSocket,
    buffer: RollingBuffer,
    detector: AnomalyDetector,
    shutdown: asyncio.Event,
) -> None:
    """Consume klines, compute features, run inference, and persist anomalies."""
    logger = structlog.get_logger(__name__)
    count = 0
    anomaly_count = 0

    async for kline in ws.stream_klines():
        if shutdown.is_set():
            break

        count += 1
        buffer.append(kline)

        features = compute_features(buffer)
        if features is None:
            continue

        is_anomaly, score = detector.predict(features)

        logger.info(
            "kline_processed",
            close=kline.close,
            volume=kline.volume,
            price_roc=round(features.price_roc, 6),
            volume_zscore=round(features.volume_zscore, 4),
            spread=round(features.spread, 2),
            anomaly=is_anomaly,
            score=round(score, 4),
            total_received=count,
        )

        if is_anomaly:
            anomaly_count += 1
            event = AnomalyEvent(
                timestamp=datetime.now(UTC),
                symbol=settings.symbol.upper(),
                price=kline.close,
                volume=kline.volume,
                features=features,
                anomaly_score=score,
            )

            logger.critical(
                "ANOMALY_ALERT",
                symbol=event.symbol,
                price=event.price,
                volume=event.volume,
                price_roc=round(features.price_roc, 6),
                volume_zscore=round(features.volume_zscore, 4),
                spread=round(features.spread, 2),
                score=round(score, 4),
                anomaly_number=anomaly_count,
            )

            await write_anomaly(event)
