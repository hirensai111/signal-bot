"""Application configuration loaded from environment via pydantic-settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/signal_bot"

    # Binance
    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_testnet: bool = True

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Risk defaults
    account_size_usdt: float = 10000.0
    risk_pct_per_trade: float = 1.0
    max_leverage: int = 20

    # Logging
    log_level: str = "INFO"

    # Watched symbols
    symbols: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT", "AVAXUSDT", "ADAUSDT"]

    # Delta Exchange India
    delta_mode: str = "testnet"  # "testnet" or "production"
    delta_api_key: str = ""
    delta_api_secret: str = ""

    # Execution
    exec_mode: str = "dry_run"  # "dry_run" or "live"

    # Risk governor limits
    daily_loss_limit_r: float = -3.0
    max_trades_per_day: int = 10
    max_concurrent_positions: int = 3
    max_consecutive_losses: int = 5
    min_balance_usdt: float = 50.0


settings = Settings()
