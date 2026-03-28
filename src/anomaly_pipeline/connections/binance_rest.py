import aiohttp
import structlog

from anomaly_pipeline.config import settings
from anomaly_pipeline.schemas import Kline

logger = structlog.get_logger(__name__)

KLINES_ENDPOINT = "/api/v3/klines"

MAX_RETRIES = 3
BASE_DELAY = 1.0


async def fetch_historical_klines(
    *,
    symbol: str = settings.symbol,
    interval: str = settings.kline_interval,
    limit: int = settings.bootstrap_limit,
    base_url: str = settings.binance_rest_url,
) -> list[Kline]:
    """Fetch historical klines from the Binance REST API for model bootstrapping.

    Returns a list of completed Kline objects sorted by open_time ascending.
    Retries up to MAX_RETRIES times with exponential backoff on failure.
    """
    url = f"{base_url}{KLINES_ENDPOINT}"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit,
    }

    import asyncio

    for attempt in range(MAX_RETRIES):
        try:
            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw_klines = await resp.json()

            klines = [
                Kline(
                    open_time=int(k[0]),
                    close_time=int(k[6]),
                    open=float(k[1]),
                    high=float(k[2]),
                    low=float(k[3]),
                    close=float(k[4]),
                    volume=float(k[5]),
                )
                for k in raw_klines
            ]

            logger.info("bootstrap_klines_fetched", count=len(klines), symbol=symbol, interval=interval)
            return klines

        except (aiohttp.ClientError, TimeoutError) as exc:
            delay = BASE_DELAY * (2**attempt)
            logger.warning(
                "bootstrap_fetch_failed",
                error=str(exc),
                attempt=attempt + 1,
                retry_in=delay,
            )
            await asyncio.sleep(delay)

    logger.error("bootstrap_fetch_exhausted", max_retries=MAX_RETRIES)
    return []
