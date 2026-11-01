"""Backtest runner: the production sweep modules replayed, deterministic execution."""

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from signal_bot.backtest.runner import BacktestRunner, replay_live_signals, synthesize_agg_trades
from signal_bot.research.data import Dataset


def _sweep_history(n_hours: int = 30) -> pd.DataFrame:
    """1m candles with a 1h swing low, then a high-volume sweep below it and a reclaim."""
    idx = pd.date_range("2024-01-01", periods=n_hours * 60, freq="1min", tz="UTC")
    rng = np.random.default_rng(0)
    base = 100 + np.cumsum(rng.normal(0, 0.02, len(idx)))
    hour = np.arange(len(idx)) // 60
    base += np.where(hour == 10, -1.5, 0.0)  # forms a clear 1h swing low around hour 10
    f = pd.DataFrame(index=idx)
    f["open"] = base
    f["close"] = base + rng.normal(0, 0.01, len(idx))
    f["high"] = np.maximum(f["open"], f["close"]) + 0.05
    f["low"] = np.minimum(f["open"], f["close"]) - 0.05
    f["volume"] = 10.0
    f["taker_buy_volume"] = 5.0
    sweep = min(len(idx) - 6, 27 * 60)  # 2024-01-02 03:00 for the full history
    f.iloc[sweep : sweep + 5, f.columns.get_loc("low")] = base.min() - 1.0
    f.iloc[sweep : sweep + 5, f.columns.get_loc("volume")] = 200.0
    f.iloc[sweep : sweep + 5, f.columns.get_loc("taker_buy_volume")] = 150.0
    f["quote_volume"] = f["volume"] * f["close"]
    f["trade_count"] = 10
    return f


def test_synthesized_trades_split_taker_flow():
    f = _sweep_history(2)
    trades = synthesize_agg_trades("BTCUSDT", f)
    buys = sum(t["qty"] for t in trades if not t["is_buyer_maker"])
    sells = sum(t["qty"] for t in trades if t["is_buyer_maker"])
    assert buys == f["taker_buy_volume"].sum()
    assert sells == (f["volume"] - f["taker_buy_volume"]).sum()


async def test_replay_uses_live_modules_and_is_deterministic():
    f = _sweep_history()
    a = await replay_live_signals("BTCUSDT", f, "5m")
    b = await replay_live_signals("BTCUSDT", f, "5m")
    assert a == b
    for s in a:
        assert s["side"] in ("long", "short")
        assert (s["stop"] < s["entry"] < s["take_profit"]) or (
            s["take_profit"] < s["entry"] < s["stop"]
        )


def test_runner_writes_deterministic_outputs(tmp_path):
    f = _sweep_history()
    ds = Dataset("BTCUSDT", "1m", f, "test")
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, 6, tzinfo=timezone.utc)
    runner = BacktestRunner("BTCUSDT", start, end, "5m", cache_dir=str(tmp_path))
    first = runner.run(ds)
    second = runner.run(ds)
    assert first == second
    assert list(tmp_path.glob("backtest_BTCUSDT_5m_*.csv"))
    assert list(tmp_path.glob("backtest_BTCUSDT_5m_*.json"))
