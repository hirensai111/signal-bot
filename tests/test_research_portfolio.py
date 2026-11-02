"""Research portfolio simulator: consistent with the single-symbol backtest, then shared capital."""

from __future__ import annotations

import json
import tomllib

import pandas as pd
import pytest

from signal_bot.research.data import Dataset, resample, synthetic
from signal_bot.research.fill_model import CostModel, Sizing, simulate
from signal_bot.research.portfolio import evaluate_portfolio, run_portfolio, summarize
from signal_bot.research.strategies import TrendBreakout
from signal_bot.risk.portfolio import PortfolioLimits

CONFIG = "signal_bot/research/strategies/trend_portfolio.toml"
LOOSE = PortfolioLimits(
    risk_per_trade=0.005,
    max_positions=100,
    max_open_risk=1.0,
    max_correlated_risk=1.0,
    max_gross_leverage=100.0,
    max_entries_per_bar=100,
    drawdown_reduce=1.0,
    drawdown_halt=1.0,
    position_leverage=1,
)


def cfg_with_universe(tmp_path, symbols, cv=1e-6, listed=None):
    with open(CONFIG, "rb") as fh:
        cfg = tomllib.load(fh)
    cfg["params"] = {**cfg["params"], "trend_ema": 50}  # synthetic data: shorter warm-up
    path = tmp_path / "universe.json"
    path.write_text(
        json.dumps(
            {
                "products": [
                    {
                        "binance_symbol": s,
                        "contract_value": cv,
                        "position_size_limit": None,
                        "launch_time": listed,
                    }
                    for s in symbols
                ]
            }
        )
    )
    cfg["universe"] = {**cfg["universe"], "contracts": str(path)}
    return cfg


def data(symbol, seed, bars=9000):
    hourly = synthetic(symbol, "1h", bars=bars, seed=seed)
    return Dataset(symbol, "4h", resample(hourly.frame, "4h"), "synthetic", True)


def test_one_symbol_with_loose_limits_reproduces_the_single_symbol_backtest(tmp_path):
    cfg = cfg_with_universe(tmp_path, ["AAAUSDT"])
    ds = data("AAAUSDT", 3, bars=16000)
    run = run_portfolio(cfg, [ds], limits=LOOSE)
    strat = TrendBreakout(**{k: v for k, v in cfg["params"].items()})
    single, _ = simulate(
        ds.frame,
        strat.generate(ds),
        CostModel(**cfg["costs"]),
        Sizing(10_000, 0.005, 1e9),
        "AAAUSDT",
    )
    a = [(t.signal_time, t.entry_time, t.exit_time, t.exit_reason) for t in run.trades if t.filled]
    b = [(t.signal_time, t.entry_time, t.exit_time, t.exit_reason) for t in single if t.filled]
    assert len(a) >= 10 and a == b  # same trades, same bars, same exits
    # prices differ only through size-dependent impact (whole-contract rounding)
    assert [t.exit_price for t in run.trades if t.filled] == pytest.approx(
        [t.exit_price for t in single if t.filled], rel=1e-6
    )
    pnl_a = [t.realized_r for t in run.trades if t.filled]
    pnl_b = [t.realized_r for t in single if t.filled]
    assert pnl_a == pytest.approx(pnl_b, rel=1e-6)  # only the tiny contract rounding differs


def test_shared_capital_ranks_simultaneous_entries_and_caps_them(tmp_path):
    cfg = cfg_with_universe(tmp_path, ["AAAUSDT", "BBBUSDT"])
    a, b = data("AAAUSDT", 3), data("BBBUSDT", 3)  # identical: every signal is simultaneous
    b = Dataset("BBBUSDT", "4h", a.frame.copy(), "synthetic", True)
    one = run_portfolio(
        cfg, [a, b], limits=PortfolioLimits(max_entries_per_bar=1, max_correlated_risk=1.0)
    )
    assert max(one.accepted_per_bar) <= 1  # never two entries at one bar close
    first = next(t for t in one.trades if t.filled)
    assert first.symbol == "AAAUSDT"  # equal strength: deterministic tie-break by symbol
    assert one.refusals["max_entries_per_bar"] >= 1
    assert sum(v for k, v in one.candidates_per_bar.items() if k > 1) >= 1
    # perfectly correlated twins: two 0.5% positions = 1% correlated risk; a 0.75% cap allows one
    capped = run_portfolio(cfg, [a, b], limits=PortfolioLimits(max_correlated_risk=0.0075))
    assert capped.refusals["correlated_risk"] >= 1


def test_symbols_cannot_trade_before_delta_listed_them(tmp_path):
    ds = data("AAAUSDT", 3)
    listed = str(ds.frame.index[len(ds.frame) // 2])
    cfg = cfg_with_universe(tmp_path, ["AAAUSDT"], listed=listed)
    run = run_portfolio(cfg, [ds], limits=LOOSE)
    assert run.refusals["not_listed_on_delta"] >= 1
    assert all(t.signal_time >= pd.Timestamp(listed) for t in run.trades)
    # metrics and windows start at the listing, not years earlier with a flat curve
    from signal_bot.research.portfolio import windows

    assert run.active_from == pd.Timestamp(listed)
    assert all(
        pd.Timestamp(w["start"], tz="UTC") >= pd.Timestamp(listed).normalize()
        for w in windows(run, 30)
    )
    unlisted = run_portfolio(cfg, [ds], limits=LOOSE, respect_listing=False)
    assert len(unlisted.trades) > len(run.trades)


def test_whole_contracts_on_marked_to_market_equity(tmp_path):
    cfg = cfg_with_universe(tmp_path, ["AAAUSDT"], cv=0.01)
    run = run_portfolio(cfg, [data("AAAUSDT", 3)], limits=LOOSE)
    for t in run.trades:
        if t.filled and not t.partial:
            assert t.qty_requested == pytest.approx(round(t.qty_requested / 0.01) * 0.01)
            assert t.extra["contracts"] == int(t.extra["contracts"])


def test_portfolio_evaluation_is_deterministic(tmp_path):
    cfg = cfg_with_universe(tmp_path, ["AAAUSDT", "BBBUSDT"])
    sets = [data("AAAUSDT", 3, 6000), data("BBBUSDT", 4, 6000)]
    x = evaluate_portfolio(cfg, sets, random_runs=3)
    y = evaluate_portfolio(cfg, sets, random_runs=3)
    assert json.dumps(x, sort_keys=True, default=str) == json.dumps(y, sort_keys=True, default=str)
    assert {c["name"] for c in x["criteria"]} and x["status"] in (
        "validated_candidate",
        "not_validated",
    )
    assert summarize(run_portfolio(cfg, sets), 10_000)["trades"] == x["summary"]["trades"]
