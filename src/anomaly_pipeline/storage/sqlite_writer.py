import aiosqlite
import structlog

from anomaly_pipeline.config import settings
from anomaly_pipeline.schemas import AnomalyEvent

logger = structlog.get_logger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS anomalies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    volume REAL NOT NULL,
    price_roc REAL NOT NULL,
    volume_zscore REAL NOT NULL,
    spread REAL NOT NULL,
    anomaly_score REAL NOT NULL
)
"""

INSERT_SQL = """
INSERT INTO anomalies (timestamp, symbol, price, volume, price_roc, volume_zscore, spread, anomaly_score)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


async def init_db(db_path: str = settings.db_path) -> None:
    """Create the anomalies table if it doesn't exist."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute(CREATE_TABLE_SQL)
        await db.commit()
    logger.info("sqlite_initialized", db_path=db_path)


async def write_anomaly(event: AnomalyEvent, db_path: str = settings.db_path) -> None:
    """Insert a detected anomaly event into the SQLite database."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            INSERT_SQL,
            (
                event.timestamp.isoformat(),
                event.symbol,
                event.price,
                event.volume,
                event.features.price_roc,
                event.features.volume_zscore,
                event.features.spread,
                event.anomaly_score,
            ),
        )
        await db.commit()

    logger.info(
        "anomaly_persisted",
        symbol=event.symbol,
        price=event.price,
        score=event.anomaly_score,
    )
