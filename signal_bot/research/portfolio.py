"""Portfolio backtest: every symbol trades from ONE account under the live portfolio rules.

The single-symbol experiments give each coin its own $10k and let every signal
trade.  That is not executable: signals arrive together, capital is shared,
crypto assets move together, contracts come in whole units and Delta lists a
fixed set of perpetuals.  This simulator replays all symbols on one 4h
timeline and, at every bar close, applies exactly what live trading applies:

* ``signals.trend.trend_decisions`` for the entries (the frozen research rule);
* ``risk.portfolio.admit`` for ranking, sizing on marked-to-market equity, whole
  Delta contracts, position limits, open/correlated risk, gross leverage,
  simultaneous-entry caps and the aggregate drawdown rules;
* ``research.fill_model.open_trade/step_trade`` for fills, costs and the stepped
  trailing stop, unchanged from the single-symbol backtest;
* Delta's universe: contract sizes and position limits from
  ``delta_universe.json``, and (by default) no trading in a symbol before Delta
  listed it.

Order of operations at each bar close *t*, identical to the live pipeline:
open positions are stepped through bar *t* (exits, trail ratchet, time stops),
equity is marked to the close, then that bar's decisions are ranked and
admitted, and accepted trades enter at the next bar's open.
"""

from __future__ import annotations

import json
import math
import random
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from signal_bot.research import metrics as M
from signal_bot.research.data import Dataset
from signal_bot.research.fill_model import (
    NO_TARGET,
    CostModel,
    Intent,
    OpenTrade,
    Trade,
    _Bars,
    open_trade,
    step_trade,
)
from signal_bot.research.strategies import frame_for
from signal_bot.risk.portfolio import (
    Candidate,
    ContractSpec,
    OpenPosition,
    PortfolioLimits,
    PortfolioState,
    admit,
    correlated_risk,
    correlations_from_returns,
    log_returns,
)
from signal_bot.signals.trend import TrendParams, atr, trend_decisions

UNIVERSE_FILE = Path(__file__).with_name("delta_universe.json")


@dataclass(frozen=True)
class UniverseEntry:
    symbol: str
    spec: ContractSpec
    listed_at: pd.Timestamp | None


def load_universe(path: str | Path | None = None) -> dict[str, UniverseEntry]:
    raw = json.loads(Path(path or UNIVERSE_FILE).read_text())
    out = {}
    for p in raw["products"]:
        limit = p.get("position_size_limit")
        out[p["binance_symbol"]] = UniverseEntry(
            p["binance_symbol"],
            ContractSpec(float(p["contract_value"]), float(limit) if limit else None),
            pd.Timestamp(p["launch_time"]) if p.get("launch_time") else None,
        )
    return out


@dataclass
class _Symbol:
    name: str
    frame: pd.DataFrame
    bars: _Bars
    pos: dict[pd.Timestamp, int]
    decisions: dict[int, Any]  # bar index -> TrendDecision or Intent-like
    entry: UniverseEntry


@dataclass
class PortfolioRun:
    trades: list[Trade] = field(default_factory=list)
    curve: list[tuple[pd.Timestamp, float]] = field(default_factory=list)
    refusals: Counter = field(default_factory=Counter)
    candidates_per_bar: Counter = field(default_factory=Counter)
    accepted_per_bar: Counter = field(default_factory=Counter)
    exposure: list[dict[str, float]] = field(default_factory=list)
    open_at_end: list[dict[str, Any]] = field(default_factory=list)


def _random_decisions(sym: _Symbol, real: dict[int, Any], params: TrendParams, rng, lo, hi):
    """Same count, random bars and sides, the strategy's own ATR stop and trail geometry."""
    a = atr(sym.frame, params.atr_n).to_numpy()
    c = sym.frame["close"].to_numpy(float)
    eligible = [i for i in range(lo, hi) if np.isfinite(a[i]) and a[i] > 0]
    k = min(len(real), len(eligible))
    out = {}
    for i in sorted(rng.sample(eligible, k)):
        side = rng.choice(["long", "short"])
        risk = params.stop_atr * a[i]
        out[i] = _Decision(
            i,
            side,
            float(c[i]),
            float(c[i] - risk if side == "long" else c[i] + risk),
            float(params.trail_atr * a[i]),
            rng.random(),
            params.max_hold_bars,
        )
    return out


@dataclass(frozen=True)
class _Decision:
    bar: int
    side: str
    entry: float
    stop: float
    trail_distance: float
    strength: float
    max_hold_bars: int


def run_portfolio(
    cfg: dict[str, Any],
    datasets: list[Dataset],
    *,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    cost_multiplier: float = 1.0,
    random_seed: int | None = None,
    respect_listing: bool | None = None,
    limits: PortfolioLimits | None = None,
    _cache: dict | None = None,
) -> PortfolioRun:
    """Replay *datasets* as one portfolio.  ``random_seed`` swaps in random entries."""
    params = TrendParams.from_mapping(cfg["params"])
    limits = limits or PortfolioLimits.from_mapping(cfg.get("portfolio"))
    cost = CostModel(**cfg.get("costs", {})).scaled(cost_multiplier)
    uni_cfg = cfg.get("universe", {})
    universe = load_universe(uni_cfg.get("contracts"))
    if respect_listing is None:
        respect_listing = bool(uni_cfg.get("respect_listing_dates", True))
    initial = float(cfg.get("sizing", {}).get("initial_equity", 10_000.0))
    cache = _cache if _cache is not None else {}

    symbols: dict[str, _Symbol] = {}
    for ds in datasets:
        if ds.symbol not in universe:
            continue  # not a Delta perpetual: not tradable
        key = ("frame", ds.symbol, ds.snapshot_id)
        if key not in cache:
            frame = frame_for(ds, params.timeframe)
            cache[key] = (
                frame,
                _Bars.of(frame),
                {t: n for n, t in enumerate(frame.index)},
                {d.bar: d for d in trend_decisions(frame, params)},
            )
        frame, bars, pos, decisions = cache[key]
        symbols[ds.symbol] = _Symbol(ds.symbol, frame, bars, pos, decisions, universe[ds.symbol])

    timeline = sorted(set().union(*(set(s.frame.index) for s in symbols.values())))
    t_start = _ts(start) if start is not None else timeline[0]
    t_end = _ts(end) if end is not None else timeline[-1] + pd.Timedelta(hours=4)

    if random_seed is not None:
        rng = random.Random(random_seed)
        for s in symbols.values():
            lo = int(s.frame.index.searchsorted(t_start))
            hi = int(s.frame.index.searchsorted(t_end)) - 1
            real = {i: d for i, d in s.decisions.items() if lo <= i < hi}
            s.decisions = _random_decisions(s, real, params, rng, max(lo, params.warmup_bars), hi)

    rkey = ("returns", tuple(sorted((s.name, id(s.frame)) for s in symbols.values())))
    if rkey not in cache:
        cache[rkey] = log_returns({s.name: s.frame["close"] for s in symbols.values()})
    rets = cache[rkey]
    corr_cache = cache.setdefault(("corr", rkey), {})

    run = PortfolioRun()
    cash = initial
    peak = initial
    open_: dict[str, OpenTrade] = {}
    for t in timeline:
        if t >= t_end:
            break
        live = t >= t_start
        # 1. step open positions through bar t
        busy: set[str] = set()
        for name, ot in list(open_.items()):
            s = symbols[name]
            k = s.pos.get(t)
            if k is None or k < ot.j:
                busy.add(name)
                continue
            busy.add(name)
            if step_trade(ot, s.bars, k, cost):
                cash += ot.trade.net_pnl
                ot.trade.equity_after = cash
                del open_[name]
        if not live:
            continue
        # 2. mark to market at the close of t
        marks = {}
        unreal = 0.0
        for name, ot in open_.items():
            s = symbols[name]
            k = s.pos.get(t)
            px = s.bars.c[k] if k is not None else ot.entry_px
            marks[name] = px
            unreal += ot.unrealized(px) - ot.fees + ot.funding
        equity = cash + unreal
        peak = max(peak, equity)
        run.curve.append((t, equity))
        positions = tuple(
            OpenPosition(n, ot.intent.side, ot.fill_qty, ot.entry_px, marks[n], ot.stop)
            for n, ot in open_.items()
        )
        # 3. this bar's decisions
        cands = []
        for name, s in symbols.items():
            i = s.pos.get(t)
            if i is None or i + 1 >= len(s.bars) or name in busy:
                continue
            d = s.decisions.get(i)
            if d is None:
                continue
            if respect_listing and s.entry.listed_at is not None and t < s.entry.listed_at:
                run.refusals["not_listed_on_delta"] += 1
                continue
            strength = d.strength
            cands.append(Candidate(name, d.side, d.entry, d.stop, strength, s.entry.spec, d))
        if not cands:
            if positions:
                run.exposure.append(_exposure(t, equity, peak, positions, {}))
            continue
        if t not in corr_cache:
            window = rets[rets.index <= t].iloc[-limits.correlation_lookback_bars :]
            corr_cache[t] = correlations_from_returns(window, limits.correlation_min_bars)
        state = PortfolioState(equity, peak, positions, corr_cache[t])
        run.candidates_per_bar[len(cands)] += 1
        accepted = 0
        for a in admit(state, cands, limits):
            if not a.accepted:
                run.refusals[(a.reason or "refused").split(":")[0]] += 1
                continue
            d, s = a.candidate.ref, symbols[a.candidate.symbol]
            it = Intent(
                d.bar,
                d.side,
                d.entry,
                d.stop,
                NO_TARGET if d.side == "long" else -NO_TARGET,
                d.max_hold_bars,
                "breakout",
                d.trail_distance,
            )
            trade, ot = open_trade(s.bars, d.bar, it, a.qty, cost, s.name)
            trade.extra = {
                "contracts": a.contracts,
                "risk_fraction": a.risk_fraction,
                "equity_at_entry": equity,
            }
            run.trades.append(trade)
            if ot is not None:
                open_[s.name] = ot
                accepted += 1
        run.accepted_per_bar[accepted] += 1
        positions = tuple(
            OpenPosition(
                n, ot.intent.side, ot.fill_qty, ot.entry_px, marks.get(n, ot.entry_px), ot.stop
            )
            for n, ot in open_.items()
        )
        run.exposure.append(_exposure(t, equity, peak, positions, corr_cache[t]))
    for name, ot in open_.items():
        run.open_at_end.append(
            {"symbol": name, "side": ot.intent.side, "entry": ot.entry_px, "stop": ot.stop}
        )
    return run


def _ts(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _exposure(t, equity, peak, positions, corr) -> dict[str, float]:
    st = PortfolioState(equity, peak, positions, corr)
    legs = [(p.symbol, p.sign * p.risk) for p in positions]
    return {
        "t": t,
        "positions": len(positions),
        "open_risk": sum(p.risk for p in positions) / equity if equity else 0.0,
        "correlated_risk": correlated_risk(st, legs) / equity if equity else 0.0,
        "gross_leverage": sum(p.notional for p in positions) / equity if equity else 0.0,
        "drawdown": st.drawdown,
    }


# ---------------------------------------------------------------------------
# summaries
# ---------------------------------------------------------------------------


def summarize(run: PortfolioRun, initial: float) -> dict[str, Any]:
    filled = [t for t in run.trades if t.filled and t.exit_time is not None]
    curve = pd.Series([e for _, e in run.curve], index=[t for t, _ in run.curve], dtype=float)
    if curve.empty:
        curve = pd.Series([initial])
    final = float(curve.iloc[-1])
    years = max((curve.index[-1] - curve.index[0]).days / 365.25, 1e-9) if len(curve) > 1 else 0
    perf = M.performance(filled, curve, initial)
    r = np.array([t.realized_r for t in filled]) if filled else np.array([])
    exp = run.exposure
    return {
        "final_equity": round(final, 2),
        "net_return_pct": round((final / initial - 1) * 100, 4),
        "cagr_pct": round(((final / initial) ** (1 / years) - 1) * 100, 4)
        if years > 0 and final > 0
        else None,
        "max_drawdown_pct": perf["max_drawdown_pct"],
        "longest_drawdown_days": perf["longest_drawdown_days"],
        "sharpe_daily_annualised": perf["sharpe_daily_annualised"],
        "trades": len(filled),
        "no_fills": sum(1 for t in run.trades if not t.filled),
        "win_rate": perf["win_rate"],
        "expectancy_r": round(float(r.mean()), 4) if len(r) else None,
        "expectancy_r_ci95": [
            round(float(r.mean() - 1.96 * r.std(ddof=1) / math.sqrt(len(r))), 4),
            round(float(r.mean() + 1.96 * r.std(ddof=1) / math.sqrt(len(r))), 4),
        ]
        if len(r) > 2
        else None,
        "profit_factor": perf["profit_factor"],
        "exit_reasons": dict(Counter(t.exit_reason for t in filled)),
        "open_at_end": run.open_at_end,
        "refusals": dict(run.refusals),
        "bars_with_candidates": sum(run.candidates_per_bar.values()),
        "bars_with_simultaneous_candidates": sum(
            v for k, v in run.candidates_per_bar.items() if k > 1
        ),
        "candidates_per_bar": {str(k): v for k, v in sorted(run.candidates_per_bar.items())},
        "accepted_per_bar": {str(k): v for k, v in sorted(run.accepted_per_bar.items())},
        "exposure": {
            "max_positions": max((e["positions"] for e in exp), default=0),
            "avg_positions_when_invested": round(
                float(np.mean([e["positions"] for e in exp if e["positions"]]))
                if any(e["positions"] for e in exp)
                else 0.0,
                3,
            ),
            "max_open_risk_pct": round(max((e["open_risk"] for e in exp), default=0) * 100, 3),
            "max_correlated_risk_pct": round(
                max((e["correlated_risk"] for e in exp), default=0) * 100, 3
            ),
            "max_gross_leverage": round(max((e["gross_leverage"] for e in exp), default=0), 3),
        },
        "by_symbol": {
            s: {
                "trades": len(ts),
                "net_pnl": round(sum(t.net_pnl for t in ts), 2),
                "expectancy_r": round(float(np.mean([t.realized_r for t in ts])), 4),
            }
            for s, ts in sorted(_group(filled).items())
        },
    }


def _group(trades):
    out: dict[str, list] = {}
    for t in trades:
        out.setdefault(t.symbol, []).append(t)
    return out


def windows(run: PortfolioRun, days: int) -> list[dict[str, Any]]:
    if not run.curve:
        return []
    curve = pd.Series([e for _, e in run.curve], index=[t for t, _ in run.curve])
    out, t0 = [], curve.index[0]
    while t0 + pd.Timedelta(days=days) <= curve.index[-1] + pd.Timedelta(hours=4):
        w = curve[(curve.index >= t0) & (curve.index < t0 + pd.Timedelta(days=days))]
        if len(w) > 1:
            out.append(
                {
                    "start": str(t0.date()),
                    "return_pct": round((w.iloc[-1] / w.iloc[0] - 1) * 100, 4),
                }
            )
        t0 += pd.Timedelta(days=days)
    return out


def evaluate_portfolio(
    cfg: dict[str, Any],
    datasets: list[Dataset],
    *,
    start=None,
    end=None,
    respect_listing=None,
    random_runs: int | None = None,
) -> dict[str, Any]:
    """Portfolio run plus cost sensitivity, rolling windows and the random-entry baseline."""
    v = cfg.get("validation", {})
    initial = float(cfg.get("sizing", {}).get("initial_equity", 10_000.0))
    cache: dict = {}
    kw = dict(start=start, end=end, respect_listing=respect_listing, _cache=cache)
    base = run_portfolio(cfg, datasets, **kw)
    summary = summarize(base, initial)
    sens = []
    for k in v.get("cost_multipliers", [0.0, 0.5, 1.0, 1.5, 2.0, 3.0]):
        s = (
            summary
            if k == 1.0
            else summarize(run_portfolio(cfg, datasets, cost_multiplier=k, **kw), initial)
        )
        sens.append(
            {
                "multiplier": k,
                "net_return_pct": s["net_return_pct"],
                "expectancy_r": s["expectancy_r"],
                "max_drawdown_pct": s["max_drawdown_pct"],
            }
        )
    runs = v.get("random_baseline_runs", 50) if random_runs is None else random_runs
    seed = int(v.get("seed", 20261102))
    rand = []
    for r in range(runs):
        s = summarize(run_portfolio(cfg, datasets, random_seed=seed + r, **kw), initial)
        rand.append(
            {
                "net_return_pct": s["net_return_pct"],
                "expectancy_r": s["expectancy_r"],
                "trades": s["trades"],
            }
        )
    win = windows(base, int(v.get("window_days", 90)))
    rets = [x["net_return_pct"] for x in rand]
    pct = (
        round(float(np.mean([summary["net_return_pct"] > x for x in rets])) * 100, 1)
        if rets
        else None
    )
    criteria = [
        {"name": "At least 30 trades", "passed": summary["trades"] >= 30},
        {
            "name": "Portfolio expectancy after costs is positive",
            "passed": (summary["expectancy_r"] or 0) > 0,
        },
        {
            "name": "At least 60% of 90-day windows non-negative",
            "passed": bool(win) and sum(w["return_pct"] >= 0 for w in win) / len(win) >= 0.6,
        },
        {
            "name": "Still profitable at 1.5x costs",
            "passed": next(
                (x["net_return_pct"] > 0 for x in sens if x["multiplier"] == 1.5), False
            ),
        },
        {
            "name": "Beats the 95th percentile of random-entry portfolios (same rules, same geometry)",
            "passed": pct is not None and pct >= 95,
        },
        {
            "name": "Max drawdown within the 20% halt level",
            "passed": summary["max_drawdown_pct"] is not None and summary["max_drawdown_pct"] > -20,
        },
    ]
    return {
        "summary": summary,
        "cost_sensitivity": sens,
        "windows": win,
        "positive_window_share": round(sum(w["return_pct"] >= 0 for w in win) / len(win), 3)
        if win
        else None,
        "random_baseline": {
            "runs": runs,
            "seed": seed,
            "mean_return_pct": round(float(np.mean(rets)), 4) if rets else None,
            "p95_return_pct": round(float(np.percentile(rets, 95)), 4) if rets else None,
            "strategy_percentile": pct,
        },
        "criteria": criteria,
        "status": "validated_candidate" if all(c["passed"] for c in criteria) else "not_validated",
        "curve": [[str(t), round(e, 2)] for t, e in base.curve[:: max(1, len(base.curve) // 600)]],
    }


def independent_baseline(
    cfg: dict[str, Any], datasets: list[Dataset], *, start=None, end=None
) -> dict[str, Any]:
    """What the per-symbol backtests imply if each coin had its own full account."""
    from signal_bot.research.fill_model import Sizing, simulate
    from signal_bot.research.strategies import TrendBreakout

    params = TrendParams.from_mapping(cfg["params"])
    cost = CostModel(**cfg.get("costs", {}))
    initial = float(cfg.get("sizing", {}).get("initial_equity", 10_000.0))
    sizing = Sizing(initial, float(cfg.get("portfolio", {}).get("risk_per_trade", 0.005)), 5.0)
    out = {}
    for ds in datasets:
        frame = frame_for(ds, params.timeframe)
        intents = TrendBreakout(**params.to_dict()).generate(ds)
        lo = frame.index.searchsorted(_ts(start)) if start else 0
        hi = frame.index.searchsorted(_ts(end)) if end else len(frame)
        intents = [i for i in intents if lo <= i.bar < hi]
        trades, curve = simulate(frame, intents, cost, sizing, ds.symbol)
        filled = [t for t in trades if t.filled]
        out[ds.symbol] = {
            "trades": len(filled),
            "net_return_pct": round((curve.iloc[-1] / initial - 1) * 100, 4),
            "expectancy_r": round(float(np.mean([t.realized_r for t in filled])), 4)
            if filled
            else None,
        }
    rets = [v["net_return_pct"] for v in out.values()]
    return {
        "per_symbol": out,
        "naive_sum_return_pct_on_one_account": round(sum(rets), 4),
        "average_return_pct": round(float(np.mean(rets)), 4) if rets else None,
        "note": "Each symbol with its own full account, every signal taken. Summing these is what treating eleven backtests as one portfolio would claim.",
    }
