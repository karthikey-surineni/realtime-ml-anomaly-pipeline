import asyncio
import signal

import structlog

from anomaly_pipeline.config import settings
from anomaly_pipeline.connections.binance_ws import BinanceWebSocket
from anomaly_pipeline.logger import setup_logging


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
    """Main async event loop: consume klines from Binance WebSocket."""
    logger = structlog.get_logger(__name__)
    loop = asyncio.get_running_loop()

    shutdown_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    ws = BinanceWebSocket()
    consumer_task = asyncio.create_task(_consume(ws, shutdown_event))

    await shutdown_event.wait()
    consumer_task.cancel()

    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    logger.info("anomaly_pipeline_shutdown_complete")


async def _consume(ws: BinanceWebSocket, shutdown: asyncio.Event) -> None:
    """Consume validated klines from the WebSocket stream and log them."""
    logger = structlog.get_logger(__name__)
    count = 0

    async for kline in ws.stream_klines():
        if shutdown.is_set():
            break

        count += 1
        logger.info(
            "kline_closed",
            open_time=kline.open_time,
            close_time=kline.close_time,
            open=kline.open,
            high=kline.high,
            low=kline.low,
            close=kline.close,
            volume=kline.volume,
            total_received=count,
        )
