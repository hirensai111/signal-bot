"""Tests for config loading."""

from signal_bot.config import Settings, settings


class TestSettings:
    def test_settings_loads_defaults(self) -> None:
        s = Settings(_env_file=None)
        assert s.account_size_usdt == 10_000.0
        assert s.risk_pct_per_trade == 1.0
        assert s.max_leverage == 20
        assert set(s.symbols) == {
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "LINKUSDT",
            "AVAXUSDT",
            "ADAUSDT",
        }

    def test_singleton_settings_available(self) -> None:
        assert isinstance(settings, Settings)
        assert settings.database_url.startswith("postgresql+asyncpg://")


def test_default_strategy_is_the_trend_portfolio_on_production_market_data():
    from signal_bot.config import Settings
    from signal_bot.main import is_trend, load_strategy_release

    s = Settings(_env_file=None)
    assert s.strategy_config.endswith("trend_portfolio.toml")
    assert s.binance_testnet is False  # market data only; the testnet feed is synthetic
    assert s.trail_mode == "stepped"
    release = load_strategy_release()
    assert release["name"] == "trend_portfolio" and is_trend(release)
    assert len(release["_doc"]["universe"]["symbols"]) == 11


def test_invalid_trailing_settings_are_refused_at_startup():
    from signal_bot.config import Settings

    s = Settings(_env_file=None, trail_mode="sometimes", delta_trail_amount_sign="maybe")
    problems = " ".join(s.validate_for_startup())
    assert "TRAIL_MODE" in problems and "DELTA_TRAIL_AMOUNT_SIGN" in problems


def test_strategy_config_resolves_inside_an_installed_package(tmp_path, monkeypatch):
    """In the Docker image the working directory is not the repository."""
    from signal_bot.main import resolve_strategy_config

    monkeypatch.chdir(tmp_path)
    path = resolve_strategy_config("signal_bot/research/strategies/trend_portfolio.toml")
    assert path.is_file() and path.name == "trend_portfolio.toml"
    import pytest

    with pytest.raises(FileNotFoundError):
        resolve_strategy_config("signal_bot/research/strategies/does_not_exist.toml")
