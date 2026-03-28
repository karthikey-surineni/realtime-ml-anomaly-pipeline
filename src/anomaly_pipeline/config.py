from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Binance connection
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
    binance_rest_url: str = "https://api.binance.com"
    symbol: str = "btcusdt"
    kline_interval: str = "1m"

    # Data pipeline
    bootstrap_limit: int = 1000
    buffer_size: int = 1000

    # ML model
    anomaly_contamination: float = 0.01

    # Storage
    db_path: str = "anomalies.db"

    # Logging
    log_level: str = "INFO"
    log_json: bool = False


settings = Settings()
