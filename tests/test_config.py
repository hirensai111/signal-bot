"""Tests for config loading."""

from config import Settings, settings


class TestSettings:
    def test_settings_loads_defaults(self) -> None:
        s = Settings(_env_file=None)
        assert s.account_size_usdt == 10_000.0
        assert s.risk_pct_per_trade == 1.0
        assert s.max_leverage == 20
        assert set(s.symbols) == {"BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT", "AVAXUSDT", "ADAUSDT"}

    def test_singleton_settings_available(self) -> None:
        assert isinstance(settings, Settings)
        assert settings.database_url.startswith("postgresql+asyncpg://")
