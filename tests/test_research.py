"""Research layer: deterministic fill model, causality, validation and reports."""

import json

import numpy as np
import pandas as pd
import pytest

from signal_bot.research.data import data_quality, resample, synthetic
from signal_bot.research.experiment import deterministic_view, load_config, run_experiment
from signal_bot.research.fill_model import CostModel, Intent, Sizing, simulate
from signal_bot.research.report import render_html
from signal_bot.research.strategies import ExhaustionReversion, RegimeFilter, build

CONFIG = "signal_bot/research/strategies/exhaustion_reversion.toml"


def _bars(rows, start="2024-01-01"):
    idx = pd.date_range(start, periods=len(rows), freq="5min", tz="UTC")
    f = pd.DataFrame(rows, columns=["open", "high", "low", "close"], index=idx, dtype=float)
    f["volume"] = 1000.0
    f["quote_volume"] = f["close"] * 1000.0
    f["trade_count"] = 10
    f["taker_buy_volume"] = 500.0
    return f


NO_COST = CostModel(0, 0, 0, 0, 1.0, 1e9, 0)


def test_long_take_profit_fills_at_limit_next_bar_entry():
    f = _bars([[100, 100, 100, 100], [100, 101, 99.5, 100.5], [100.5, 102.5, 100, 102]])
    trades, eq = simulate(f, [Intent(0, "long", 100, 99, 102)], NO_COST, Sizing(10_000, 0.01, 100))
    [t] = trades
    assert t.entry_price == 100 and t.exit_reason == "tp" and t.exit_price == 102
    assert t.signal_r == pytest.approx(2.0) and t.realized_r == pytest.approx(2.0)
    assert eq.iloc[-1] == pytest.approx(10_200)


def test_stop_gap_fills_at_open_not_stop():
    f = _bars([[100, 100, 100, 100], [100, 100.2, 99.8, 100], [97, 97.5, 96.5, 97]])
    [t], _ = simulate(f, [Intent(0, "long", 100, 99, 103)], NO_COST, Sizing(10_000, 0.01, 100))
    assert t.exit_reason == "sl" and t.exit_price == 97  # gapped through 99
    assert t.realized_r == pytest.approx(-3.0)
    assert t.signal_r == pytest.approx(-1.0)  # the idea lost 1R; the gap cost 2R more


def test_same_bar_stop_and_target_assumes_stop_first():
    f = _bars([[100, 100, 100, 100], [100, 103, 98, 101]])
    [t], _ = simulate(f, [Intent(0, "long", 100, 99, 102)], NO_COST, Sizing())
    assert t.exit_reason == "sl"


def test_gap_beyond_entry_tolerance_is_a_deterministic_no_fill():
    f = _bars([[100, 100, 100, 100], [101, 101, 101, 101]])
    cost = CostModel(max_entry_slippage_bps=15)
    [t], _ = simulate(f, [Intent(0, "long", 100, 99, 102)], cost, Sizing())
    assert t.exit_reason == "no_fill_gap" and not t.filled


def test_participation_cap_causes_partial_fill():
    f = _bars([[100, 100, 100, 100], [100, 100.5, 99.9, 100], [100, 102.5, 100, 102]])
    f["quote_volume"] = 10_000.0  # thin bars
    cost = CostModel(0, 0, 0, 0, 0.05, 1e9, 0)
    [t], _ = simulate(f, [Intent(0, "long", 100, 99, 102)], cost, Sizing(10_000, 0.01, 100))
    assert t.partial and t.qty_filled == pytest.approx(5.0)  # 5% of $10k at $100
    assert t.realized_r < t.signal_r  # partial size realises less of the idea


def test_costs_scale_monotonically():
    ds = synthetic(bars=6000, seed=3)
    strat = ExhaustionReversion(run_atr=1.5, climax_vol_z=1.0)
    intents = strat.generate(ds)
    exps = []
    for k in (0, 1, 2):
        trades, _ = simulate(ds.frame, intents, CostModel().scaled(k), Sizing())
        exps.append(np.mean([t.realized_r for t in trades if t.filled]))
    assert exps[0] > exps[1] > exps[2]


def test_strategy_is_causal():
    """Appending future bars must not change earlier decisions."""
    ds = synthetic(bars=8000, seed=11)
    strat = ExhaustionReversion(run_atr=1.5, climax_vol_z=1.0)
    early = strat.generate(ds.slice(end=ds.frame.index[5000]))
    full = [i for i in strat.generate(ds) if i.bar < 5000 - 1]
    assert [(i.bar, i.side) for i in early if i.bar < 5000 - 1] == [(i.bar, i.side) for i in full]


def test_regime_filter_only_removes_trades():
    ds = synthetic(bars=8000, seed=5)
    inner = ExhaustionReversion(run_atr=1.5, climax_vol_z=1.0)
    filt = RegimeFilter(ExhaustionReversion(run_atr=1.5, climax_vol_z=1.0))
    a, b = inner.generate(ds), filt.generate(ds)
    assert set((i.bar, i.side) for i in b) <= set((i.bar, i.side) for i in a)
    assert filt.stats["candidates"] == len(a) and filt.stats["kept"] == len(b)


def test_data_quality_reports_gaps_and_resample():
    ds = synthetic(interval="1m", bars=600, seed=1)
    holey = ds.frame.drop(ds.frame.index[100:110])
    q = data_quality(holey, "1m")
    assert q["gap_count"] == 1 and q["missing_bars"] == 10 and q["coverage"] < 1
    five = resample(ds.frame, "5m")
    assert len(five) == 120 and five["volume"].sum() == pytest.approx(ds.frame["volume"].sum())


@pytest.mark.replay
def test_experiment_is_deterministic_and_report_renders():
    cfg = load_config(CONFIG)
    cfg["validation"] = {**cfg["validation"], "random_baseline_runs": 5}
    cfg["walk_forward"] = {"grid": {"run_atr": [2.0, 2.5]}}
    data = [
        synthetic("AAAUSDT", "5m", bars=12_000, seed=7),
        synthetic("BBBUSDT", "5m", bars=12_000, seed=8),
    ]
    a = run_experiment(cfg, data)
    b = run_experiment(cfg, data)
    assert json.dumps(deterministic_view(a), sort_keys=True, default=str) == json.dumps(
        deterministic_view(b), sort_keys=True, default=str
    )
    assert a["report_id"] == b["report_id"]
    for section in ("strategy", "data", "results", "validation", "reliability", "provenance"):
        assert section in a
    assert a["data"][0]["synthetic"] is True
    html = render_html(a)
    assert "Synthetic data" in html and "<svg" in html and "Walk-forward" in html


def test_build_wraps_regime_filter_from_config():
    cfg = load_config(CONFIG)
    strat = build(cfg)
    assert isinstance(strat, RegimeFilter)
    assert strat.params()["regime_filter"]["min_vol_pct"] == 0.2
    assert len(cfg["_hash"]) == 16
