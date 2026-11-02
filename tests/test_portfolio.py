"""Portfolio rules shared by live trading and the research portfolio simulator."""

from __future__ import annotations

import math
import tomllib

import numpy as np
import pandas as pd
import pytest

from signal_bot.risk.portfolio import (
    Candidate,
    ContractSpec,
    OpenPosition,
    PortfolioLimits,
    PortfolioState,
    admit,
    correlated_risk,
    exposure_summary,
    rank,
    return_correlations,
)

CV1 = ContractSpec(1.0)


def cand(sym, side="long", entry=100.0, stop=95.0, strength=1.0, spec=CV1):
    return Candidate(sym, side, entry, stop, strength, spec)


def state(equity=10_000.0, peak=None, positions=(), corr=None):
    return PortfolioState(equity, peak or equity, tuple(positions), corr or {})


def test_risk_is_a_fraction_of_shared_equity_in_whole_contracts():
    [a] = admit(state(10_000), [cand("A", entry=100, stop=95)], PortfolioLimits())
    # 0.5% of 10k = $50 at $5/unit = 10 units
    assert a.accepted and a.contracts == 10 and a.risk_amount == pytest.approx(50)
    btc = ContractSpec(0.001)
    [b] = admit(
        state(10_000), [cand("BTC", entry=65_000, stop=63_700, spec=btc)], PortfolioLimits()
    )
    # $50 / $1300 = 0.03846 BTC = 38.46 contracts of 0.001 -> 38 (rounded down)
    assert b.contracts == 38 and b.risk_amount == pytest.approx(38 * 0.001 * 1300)


def test_a_position_smaller_than_one_contract_is_refused():
    doge = ContractSpec(100.0)  # Delta DOGEUSD: 100 DOGE per contract
    [a] = admit(state(1_000), [cand("DOGE", entry=0.1, stop=0.095, spec=doge)], PortfolioLimits())
    # $5 risk / $0.005 = 1000 DOGE = 10 contracts
    assert a.accepted and a.contracts == 10
    [b] = admit(state(50), [cand("DOGE", entry=0.1, stop=0.095, spec=doge)], PortfolioLimits())
    assert not b.accepted and b.reason == "below_min_contract"  # $0.25 risk = 50 DOGE


def test_venue_position_limit_caps_the_size():
    capped = ContractSpec(1.0, max_contracts=3)
    [a] = admit(state(10_000), [cand("A", spec=capped)], PortfolioLimits())
    assert a.contracts == 3


def test_simultaneous_entries_are_ranked_and_capped_per_bar():
    cands = [
        cand("C", strength=0.5),
        cand("A", strength=2.0),
        cand("B", strength=2.0),
        cand("D", strength=0.1),
    ]
    assert [c.symbol for c in rank(cands)] == ["A", "B", "C", "D"]
    limits = PortfolioLimits(max_entries_per_bar=2, max_correlated_risk=1.0)
    out = admit(state(), cands, limits)
    assert [(a.candidate.symbol, a.accepted, a.reason) for a in out] == [
        ("A", True, None),
        ("B", True, None),
        ("C", False, "max_entries_per_bar"),
        ("D", False, "max_entries_per_bar"),
    ]


def test_one_position_per_symbol_and_a_position_cap():
    pos = [OpenPosition("A", "long", 10, 100, 100, 95)]
    [a] = admit(state(positions=pos), [cand("A")], PortfolioLimits())
    assert a.reason == "symbol_exposed"
    full = [OpenPosition(f"S{i}", "long", 1, 100, 100, 99) for i in range(6)]
    [b] = admit(state(positions=full, corr=_zero_corr(7)), [cand("NEW")], PortfolioLimits())
    assert b.reason == "max_positions"


def _zero_corr(n):
    names = [f"S{i}" for i in range(n - 1)] + ["NEW", "A", "B", "C", "D", "E"]
    return {(a, b): 0.0 for i, a in enumerate(names) for b in names[i + 1 :]}


def test_total_open_risk_is_capped_using_current_stops():
    # 5 open positions with $60 each to their stops = $300 (3% of 10k): no room left
    pos = [OpenPosition(f"S{i}", "long", 12, 100, 100, 95) for i in range(5)]
    [a] = admit(state(positions=pos, corr=_zero_corr(6)), [cand("NEW")], PortfolioLimits())
    assert a.reason == "max_open_risk"
    # risk is measured from the mark to the CURRENT stop: trailed-up stops free budget
    trailed = [OpenPosition(f"S{i}", "long", 12, 100, 120, 118) for i in range(5)]
    [b] = admit(state(positions=trailed, corr=_zero_corr(6)), [cand("NEW")], PortfolioLimits())
    assert b.accepted  # 5 x $24 + $50 = $170 <= $300


def test_correlated_same_side_risk_is_limited_and_hedges_offset():
    syms = ["A", "B", "C", "D", "E"]
    corr = {(a, b): 0.7 for i, a in enumerate(syms) for b in syms[i + 1 :]}
    limits = PortfolioLimits(max_entries_per_bar=10, max_open_risk=1.0)
    out = admit(state(corr=corr), [cand(s, strength=5 - i) for i, s in enumerate(syms)], limits)
    # r * sqrt(k + k(k-1) rho) with r = 0.5%: k=4 -> 1.76% <= 2%, k=5 -> 2.18% > 2%
    assert [a.accepted for a in out] == [True, True, True, True, False]
    assert out[-1].reason == "correlated_risk"
    # the fifth as a SHORT partly offsets the longs and is allowed
    out = admit(
        state(corr=corr),
        [cand(s, strength=5 - i) for i, s in enumerate(syms[:4])]
        + [cand("E", side="short", stop=105, strength=0)],
        limits,
    )
    assert all(a.accepted for a in out)


def test_unknown_correlations_are_treated_as_perfect():
    st = state()
    assert correlated_risk(st, [("A", 50.0), ("B", 50.0)]) == pytest.approx(100.0)
    st = state(corr={("A", "B"): 0.0})
    assert correlated_risk(st, [("A", 50.0), ("B", 50.0)]) == pytest.approx(math.sqrt(2) * 50)


def test_gross_leverage_cap():
    # tight 0.25% stop: $50 risk -> $20k notional = 2x; a second one would make 4x > 3x
    limits = PortfolioLimits(max_correlated_risk=1.0)
    out = admit(
        state(corr={("A", "B"): 0.0}),
        [cand("A", stop=99.75, strength=2), cand("B", stop=99.75)],
        limits,
    )
    assert out[0].accepted and out[1].reason == "max_gross_leverage"


def test_drawdown_halves_risk_then_stops_entries():
    limits = PortfolioLimits()
    [a] = admit(state(9_000, peak=10_000), [cand("A")], limits)  # 10% drawdown
    assert a.accepted and a.risk_fraction == pytest.approx(0.0025)
    [b] = admit(state(8_000, peak=10_000), [cand("A")], limits)  # 20%
    assert not b.accepted and b.reason.startswith("portfolio_drawdown_halt")
    summary = exposure_summary(state(8_000, peak=10_000), limits)
    assert summary["entries_allowed"] is False and summary["drawdown_pct"] == pytest.approx(20)


def test_a_stop_beyond_half_the_liquidation_distance_is_refused():
    # 5x isolated margin liquidates near a 20% move; stops beyond 10% are refused
    [a] = admit(state(), [cand("A", entry=100, stop=89)], PortfolioLimits(position_leverage=5))
    assert a.reason == "stop_beyond_margin_buffer"


def test_return_correlations_are_causal_and_need_enough_overlap():
    idx = pd.date_range("2025-01-01", periods=300, freq="4h", tz="UTC")
    rng = np.random.default_rng(1)
    base = np.cumsum(rng.normal(0, 0.01, 300))
    a = pd.Series(100 * np.exp(base), index=idx)
    b = pd.Series(100 * np.exp(base + rng.normal(0, 0.002, 300)), index=idx)
    c = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.01, 300))), index=idx)
    corr = return_correlations({"A": a, "B": b, "C": c}, lookback=180, min_bars=60)
    assert corr[("A", "B")] > 0.9 and abs(corr[("A", "C")]) < 0.3
    short = return_correlations({"A": a.iloc[:30], "B": b.iloc[:30]}, 180, 60)
    assert short == {}  # too little overlap: treated as 1 by the rules


def test_portfolio_config_runs_the_frozen_pre_registered_signal():
    with open("signal_bot/research/strategies/trend_portfolio.toml", "rb") as fh:
        portfolio = tomllib.load(fh)
    with open(portfolio["signal_config"], "rb") as fh:
        signal = tomllib.load(fh)
    assert portfolio["params"] == signal["params"]
    assert portfolio["strategy"] == signal["strategy"] == "trend_breakout"
    PortfolioLimits.from_mapping(portfolio["portfolio"])  # every key is a real limit
    assert set(portfolio["portfolio"]) <= set(PortfolioLimits().to_dict())


def test_governor_defaults_follow_the_portfolio_and_keep_stricter_operator_caps():
    from signal_bot.risk.portfolio import governor_defaults

    limits = PortfolioLimits()
    d = governor_defaults(limits, {})
    assert d == {
        "max_concurrent_positions": 6,
        "daily_loss_limit_r": pytest.approx(-6.0),
        "max_trades_per_day": 18,
        "max_consecutive_losses": 15,
    }
    # an operator who set MAX_CONCURRENT_POSITIONS=1 and DAILY_LOSS_LIMIT_R=-2 keeps them
    d = governor_defaults(
        limits, {"max_concurrent_positions": 1, "daily_loss_limit_r": -2.0, "log_level": "INFO"}
    )
    assert d["max_concurrent_positions"] == 1 and d["daily_loss_limit_r"] == -2.0
    # a looser explicit value cannot exceed the researched portfolio
    assert (
        governor_defaults(limits, {"max_concurrent_positions": 20})["max_concurrent_positions"] == 6
    )


def test_operator_caps_only_tighten_the_researched_limits():
    from signal_bot.risk.portfolio import apply_operator_caps

    limits = PortfolioLimits()
    capped = apply_operator_caps(
        limits, {"risk_pct_per_trade": 0.25, "max_concurrent_positions": 1}
    )
    assert capped.risk_per_trade == pytest.approx(0.0025) and capped.max_positions == 1
    loose = apply_operator_caps(limits, {"risk_pct_per_trade": 1.0, "max_concurrent_positions": 9})
    assert loose == limits
    assert apply_operator_caps(limits, {}) == limits  # unset settings never apply
