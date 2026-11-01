"""Application configuration loaded from environment via pydantic-settings."""

import hashlib
import json
import logging

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# Never included in provenance hashes, reports or the status API.
SECRET_FIELDS = frozenset(
    {
        "database_url",
        "binance_api_key",
        "binance_api_secret",
        "telegram_bot_token",
        "telegram_chat_id",
        "delta_api_key",
        "delta_api_secret",
        "operator_token",
    }
)


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
    log_format: str = "json"  # "json" | "text"

    # Watched symbols
    symbols: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT", "AVAXUSDT", "ADAUSDT"]

    # Delta Exchange India
    delta_mode: str = "testnet"  # "testnet" or "production"
    delta_api_key: str = ""
    delta_api_secret: str = ""
    delta_ws_origin: str = ""  # overridden by model_validator below
    delta_ws_url: str = ""  # override the documented private socket for DELTA_MODE
    delta_ws_auth_path: str = "/live"  # key-auth signs "GET" + timestamp + this path

    # Execution: "paper" (realistic simulated fills on live Delta books) or "live".
    # "dry_run" is accepted as a deprecated alias for "paper".
    exec_mode: str = "paper"
    # Required in addition to exec_mode=live when delta_mode=production.
    live_trading_acknowledged: bool = False
    order_type: str = "limit_ioc"
    no_fill_timeout_seconds: float = 60.0
    max_candle_lag_seconds: float = 90.0
    max_book_age_seconds: float = 10.0

    # Paper broker
    paper_initial_equity: float | None = None  # defaults to account_size_usdt
    paper_taker_fee: float = 0.0005
    paper_maker_fee: float = 0.0002
    paper_latency_ms: float = 150.0
    paper_funding_rate: float = 0.0001
    paper_state_path: str = "data/paper_state.json"

    # Risk governor limits
    daily_loss_limit_r: float = -3.0
    max_trades_per_day: int = 10
    max_concurrent_positions: int = 3
    max_consecutive_losses: int = 5
    min_balance_usdt: float = 50.0

    # Reconciliation
    reconcile_interval_seconds: float = 300.0
    unknown_order_grace_seconds: float = 300.0
    block_on_orphan_positions: bool = True

    # Ops server (metrics, health, control room)
    ops_host: str = "127.0.0.1"
    ops_port: int = 8080
    operator_token: str = ""  # empty => control endpoints disabled

    # Strategy release
    strategy_config: str = "signal_bot/research/strategies/liquidity_sweep.toml"

    @model_validator(mode="after")
    def set_default_ws_origin(self):
        if not self.delta_ws_origin:
            self.delta_ws_origin = (
                "https://demo.delta.exchange"
                if self.delta_mode == "testnet"
                else "https://india.delta.exchange"
            )
        if self.exec_mode == "dry_run":
            self.exec_mode = "paper"
        return self

    def public_dict(self) -> dict:
        """Settings with secrets removed (safe for reports, logs and the status API)."""
        return {k: v for k, v in self.model_dump().items() if k not in SECRET_FIELDS}

    def fingerprint(self) -> str:
        """Stable hash of the non-secret configuration, recorded with every run."""
        blob = json.dumps(self.public_dict(), sort_keys=True, default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    def validate_for_startup(self) -> list[str]:
        """Return fatal configuration problems (empty list = OK to start)."""
        problems = []
        if self.exec_mode not in ("paper", "live"):
            problems.append(f"EXEC_MODE must be paper or live, got {self.exec_mode!r}")
        if self.exec_mode == "live":
            if not (self.delta_api_key and self.delta_api_secret):
                problems.append("EXEC_MODE=live requires DELTA_API_KEY and DELTA_API_SECRET")
            if self.delta_mode == "production" and not self.live_trading_acknowledged:
                problems.append(
                    "Refusing production trading: set LIVE_TRADING_ACKNOWLEDGED=true after "
                    "completing the go-live checklist in docs/runbook.md"
                )
        if self.ops_host not in ("127.0.0.1", "localhost", "::1") and not self.operator_token:
            problems.append(
                "OPS_HOST is non-local; set OPERATOR_TOKEN to protect control endpoints"
            )
        return problems


settings = Settings()
