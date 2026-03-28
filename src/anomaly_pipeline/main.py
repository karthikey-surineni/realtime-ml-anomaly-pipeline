import asyncio

import structlog

from anomaly_pipeline.config import settings
from anomaly_pipeline.logger import setup_logging


def main() -> None:
    """Entry point for the anomaly-pipeline CLI command."""
    setup_logging(log_level=settings.log_level)
    logger = structlog.get_logger(__name__)
    logger.info("anomaly_pipeline_starting", symbol=settings.symbol, interval=settings.kline_interval)
    asyncio.run(_run())


async def _run() -> None:
    """Main async event loop — placeholder for Phase 2."""
    logger = structlog.get_logger(__name__)
    logger.info("pipeline_ready", msg="Phase 1 scaffolding complete. Awaiting Phase 2 implementation.")
