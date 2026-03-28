import asyncio
import json
from collections.abc import AsyncIterator

import structlog
import websockets
from pydantic import ValidationError

from anomaly_pipeline.config import settings
from anomaly_pipeline.schemas import BinanceKlineEvent, Kline

logger = structlog.get_logger(__name__)

MAX_RETRIES = 10
BASE_DELAY = 1.0
MAX_DELAY = 60.0


class BinanceWebSocket:
    """WebSocket client for Binance kline stream with exponential backoff reconnection."""

    def __init__(self, url: str = settings.binance_ws_url) -> None:
        self._url = url
        self._retry_count = 0

    def _backoff_delay(self) -> float:
        delay = min(BASE_DELAY * (2 ** self._retry_count), MAX_DELAY)
        self._retry_count += 1
        return delay

    def _reset_retries(self) -> None:
        self._retry_count = 0

    async def stream_klines(self) -> AsyncIterator[Kline]:
        """Yield validated Kline objects from the Binance WebSocket stream.

        Reconnects with exponential backoff on connection failures.
        Only yields completed (closed) klines.
        """
        while self._retry_count < MAX_RETRIES:
            try:
                async for websocket in websockets.connect(self._url):
                    try:
                        logger.info("binance_ws_connected", url=self._url)
                        self._reset_retries()

                        async for raw_message in websocket:
                            kline = self._parse_message(raw_message)
                            if kline is not None:
                                yield kline

                    except websockets.ConnectionClosed as exc:
                        delay = self._backoff_delay()
                        logger.warning(
                            "binance_ws_disconnected",
                            code=exc.code,
                            reason=exc.reason,
                            retry_in=delay,
                            retry_count=self._retry_count,
                        )
                        await asyncio.sleep(delay)

            except OSError as exc:
                delay = self._backoff_delay()
                logger.error(
                    "binance_ws_connection_failed",
                    error=str(exc),
                    retry_in=delay,
                    retry_count=self._retry_count,
                )
                await asyncio.sleep(delay)

        logger.critical("binance_ws_max_retries_exceeded", max_retries=MAX_RETRIES)

    def _parse_message(self, raw: str) -> Kline | None:
        """Validate a raw WebSocket message and return a Kline if the candle is closed."""
        try:
            data = json.loads(raw)
            event = BinanceKlineEvent.model_validate(data)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning("binance_ws_invalid_message", error=str(exc))
            return None

        if not event.k.x:
            return None

        kline = Kline.from_binance_event(event)
        logger.debug(
            "kline_received",
            open_time=kline.open_time,
            close=kline.close,
            volume=kline.volume,
        )
        return kline
