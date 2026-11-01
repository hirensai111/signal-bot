"""Reproducible experiments: IS/OOS, walk-forward, cost sensitivity, baselines.

An experiment is fully determined by (strategy config file, data snapshot ids,
code commit, seed).  All of them are written into the report.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import logging
import random
import tomllib
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from signal_bot.provenance import provenance
from signal_bot.research import metrics as M
from signal_bot.research.data import Dataset
from signal_bot.research.fill_model import NO_TARGET, CostModel, Intent, Sizing, Trade, simulate
from signal_bot.research.strategies import build, frame_for

logger = logging.getLogger(__name__)

REPORT_SCHEMA_VERSION = 1

DEFAULT_VALIDATION = {
    "oos_fraction": 0.33,
    "walk_forward_train_days": 90,
    "walk_forward_test_days": 30,
    "cost_multipliers": [0.0, 0.5, 1.0, 1.5, 2.0, 3.0],
    "random_baseline_runs": 50,
    "seed": 20261101,
    "min_oos_trades": 30,
    "min_break_even_cost_multiplier": 1.5,
    "min_positive_wf_windows": 0.6,
}


def load_config(path: str | Path) -> dict[str, Any]:
    raw = Path(path).read_bytes()
    cfg = tomllib.loads(raw.decode())
    cfg["_path"] = str(path)
    cfg["_hash"] = config_hash(cfg)
    return cfg


def config_hash(cfg: dict[str, Any]) -> str:
    clean = {k: v for k, v in cfg.items() if not k.startswith("_")}
    return hashlib.sha256(json.dumps(clean, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _costs(cfg) -> CostModel:
    return CostModel(**cfg.get("costs", {}))


def _sizing(cfg) -> Sizing:
    return Sizing(**cfg.get("sizing", {}))


def _validation(cfg) -> dict[str, Any]:
    return {**DEFAULT_VALIDATION, **cfg.get("validation", {})}


def _window(intents: list[Intent], frame: pd.DataFrame, start, end) -> list[Intent]:
    t = frame.index
    return [it for it in intents if start <= t[it.bar] < end]


def _run(frame, intents, cost, sizing, symbol, start=None, end=None):
    if start is None:
        return simulate(frame, intents, cost, sizing, symbol)
    # Simulate only the window (plus room for the longest hold) instead of the full frame.
    sel = _window(intents, frame, start, end)
    lo = int(frame.index.searchsorted(start))
    hi = int(frame.index.searchsorted(end))
    tail = max((it.max_hold_bars for it in sel), default=0) + 2
    sub = frame.iloc[lo : min(len(frame), hi + tail)]
    shifted = [replace(it, bar=it.bar - lo) for it in sel]
    trades, equity = simulate(sub, shifted, cost, sizing, symbol)
    equity = equity[(equity.index >= start) & (equity.index < end)]
    if equity.empty:
        equity = pd.Series([sizing.initial_equity], index=[start])
    return trades, equity


def _buy_hold(frame: pd.DataFrame, start=None, end=None) -> float:
    f = frame if start is None else frame[(frame.index >= start) & (frame.index < end)]
    if f.empty:
        return 0.0
    return round(float(f["close"].iloc[-1] / f["open"].iloc[0] - 1) * 100, 4)


def _random_baseline(
    frame, trades: list[Trade], cost, sizing, symbol, runs: int, seed: int, first_bar: int = 0
):
    filled = [t for t in trades if t.filled]
    if len(filled) < 5:
        return None
    stop_pct = float(np.median([abs(t.intended_entry - t.stop) / t.intended_entry for t in filled]))
    finite = [t for t in filled if np.isfinite(t.take_profit)]
    rr = (
        float(
            np.median(
                [
                    abs(t.take_profit - t.intended_entry) / abs(t.intended_entry - t.stop)
                    for t in finite
                ]
            )
        )
        if finite
        else None
    )
    trails = [t.trail_distance / t.intended_entry for t in filled if t.trail_distance]
    trail_pct = float(np.median(trails)) if trails else None
    hold = (
        int(
            np.median(
                [(t.exit_time - t.entry_time) / (frame.index[1] - frame.index[0]) for t in filled]
            )
        )
        + 1
    )
    closes = frame["close"].to_numpy(float)
    exp = []
    for r in range(runs):
        rng = random.Random(seed + r)
        pool = range(max(50, first_bar), len(frame) - 2)
        bars = sorted(rng.sample(pool, k=min(len(filled), len(pool) - 8)))
        intents = []
        for b in bars:
            side = rng.choice(["long", "short"])
            e = closes[b]
            stop = e * (1 - stop_pct) if side == "long" else e * (1 + stop_pct)
            if rr is not None:
                tp = e + rr * (e - stop) if side == "long" else e - rr * (stop - e)
            else:
                tp = NO_TARGET if side == "long" else -NO_TARGET
            trail = e * trail_pct if trail_pct else None
            intents.append(Intent(b, side, e, stop, tp, max(hold * 3, 2), "random", trail))
        rt, req = simulate(frame, intents, cost, sizing, symbol)
        rf = [t.realized_r for t in rt if t.filled]
        if rf:
            exp.append(float(np.mean(rf)))
    if not exp:
        return None
    strat = float(np.mean([t.realized_r for t in filled]))
    return {
        "runs": len(exp),
        "seed": seed,
        "mean_expectancy_r": round(float(np.mean(exp)), 4),
        "p05_expectancy_r": round(float(np.percentile(exp, 5)), 4),
        "p95_expectancy_r": round(float(np.percentile(exp, 95)), 4),
        "strategy_expectancy_r": round(strat, 4),
        "strategy_percentile": round(float(np.mean([strat > e for e in exp])) * 100, 1),
        "geometry": {
            "stop_pct": round(stop_pct, 5),
            "reward_risk": None if rr is None else round(rr, 3),
            "trail_pct": None if trail_pct is None else round(trail_pct, 5),
            "hold_bars": hold,
        },
    }


def _break_even(points: list[dict]) -> float | None:
    pts = sorted(points, key=lambda p: p["multiplier"])
    for a, b in itertools.pairwise(pts):
        ea, eb = a["expectancy_r"], b["expectancy_r"]
        if ea is None or eb is None:
            continue
        if ea > 0 >= eb:
            return round(a["multiplier"] + (b["multiplier"] - a["multiplier"]) * ea / (ea - eb), 3)
    if pts and all((p["expectancy_r"] or 0) > 0 for p in pts):
        return float("inf")
    if pts and (pts[0]["expectancy_r"] or 0) <= 0:
        return 0.0
    return None


def _grid(cfg) -> list[dict[str, Any]]:
    grid = cfg.get("walk_forward", {}).get("grid", {})
    if not grid:
        return [{}]
    keys = sorted(grid)
    return [
        dict(zip(keys, combo, strict=True)) for combo in itertools.product(*(grid[k] for k in keys))
    ]


def _with_params(cfg, overrides):
    return {**cfg, "params": {**cfg.get("params", {}), **overrides}}


def run_symbol(cfg: dict[str, Any], data: Dataset) -> dict[str, Any]:
    v = _validation(cfg)
    cost, sizing = _costs(cfg), _sizing(cfg)
    strategy = build(cfg)
    frame = frame_for(data, strategy.timeframe)
    intents = strategy.generate(data)
    filter_stats = getattr(strategy, "stats", None)
    start, end = frame.index[0], frame.index[-1] + (frame.index[1] - frame.index[0])
    # evaluation_start: bars before it only warm up indicators (no trades, no metrics).
    warm = v.get("evaluation_start")
    if warm is not None:
        start = max(start, pd.Timestamp(warm, tz=frame.index.tz))
    window = () if warm is None else (start, end)
    split = start + (end - start) * (1 - v["oos_fraction"])
    split = split.floor("D")

    full_t, full_e = _run(frame, intents, cost, sizing, data.symbol, *window)
    is_t, is_e = _run(frame, intents, cost, sizing, data.symbol, start, split)
    oos_t, oos_e = _run(frame, intents, cost, sizing, data.symbol, split, end)

    # walk-forward: choose params on each train window, evaluate on the next test window
    grid = _grid(cfg)
    cache: dict[int, list[Intent]] = {}

    def intents_for(k: int) -> list[Intent]:
        if k not in cache:
            cache[k] = (
                intents if grid[k] == {} else build(_with_params(cfg, grid[k])).generate(data)
            )
        return cache[k]

    train, test = (
        pd.Timedelta(days=v["walk_forward_train_days"]),
        pd.Timedelta(days=v["walk_forward_test_days"]),
    )
    windows, wf_trades = [], []
    t0 = start.floor("D")
    while t0 + train + test <= end + pd.Timedelta(days=1):
        tr_s, tr_e, te_e = t0, t0 + train, min(t0 + train + test, end)
        best_k, best_score = 0, -np.inf
        for k in range(len(grid)):
            tt, _ = _run(frame, intents_for(k), cost, sizing, data.symbol, tr_s, tr_e)
            filled = [t.realized_r for t in tt if t.filled]
            score = float(np.mean(filled)) if len(filled) >= 10 else -np.inf
            if score > best_score:
                best_k, best_score = k, score
        te_t, te_e_eq = _run(frame, intents_for(best_k), cost, sizing, data.symbol, tr_e, te_e)
        perf = M.performance(te_t, te_e_eq, sizing.initial_equity)
        windows.append(
            {
                "train": [str(tr_s.date()), str(tr_e.date())],
                "test": [str(tr_e.date()), str(te_e.date())],
                "params": grid[best_k],
                "train_expectancy_r": None if not np.isfinite(best_score) else round(best_score, 4),
                "test_trades": perf["trades"],
                "test_expectancy_r": perf["expectancy_r"],
                "test_return_pct": perf["net_return_pct"],
            }
        )
        wf_trades += [t for t in te_t if t.filled]
        t0 += test

    sens = []
    for k in v["cost_multipliers"]:
        t, e = _run(frame, intents, cost.scaled(k), sizing, data.symbol, *window)
        p = M.performance(t, e, sizing.initial_equity)
        sens.append(
            {
                "multiplier": k,
                "net_return_pct": p["net_return_pct"],
                "expectancy_r": p["expectancy_r"],
                "profit_factor": p["profit_factor"],
            }
        )

    wf_exp = float(np.mean([t.realized_r for t in wf_trades])) if wf_trades else None
    tested = [w for w in windows if w["test_trades"]]
    return {
        "symbol": data.symbol,
        "timeframe": strategy.timeframe,
        "windows": {
            "full": [str(start), str(end)],
            "warmup_from": None if warm is None else str(frame.index[0]),
            "in_sample": [str(start), str(split)],
            "out_of_sample": [str(split), str(end)],
        },
        "regime_filter": filter_stats,
        "performance": {
            "full": M.performance(full_t, full_e, sizing.initial_equity),
            "in_sample": M.performance(is_t, is_e, sizing.initial_equity),
            "out_of_sample": M.performance(oos_t, oos_e, sizing.initial_equity),
        },
        "monthly": M.monthly(full_t, full_e),
        "walk_forward": {
            "windows": windows,
            "trades": len(wf_trades),
            "expectancy_r": None if wf_exp is None else round(wf_exp, 4),
            "positive_window_share": round(
                sum((w["test_expectancy_r"] or 0) > 0 for w in tested) / len(tested), 3
            )
            if tested
            else None,
        },
        "cost_sensitivity": {"points": sens, "break_even_multiplier": _break_even(sens)},
        "baselines": {
            "buy_and_hold_pct": {
                "full": _buy_hold(frame, *window),
                "out_of_sample": _buy_hold(frame, split, end),
            },
            "random_entries": _random_baseline(
                frame,
                full_t,
                cost,
                sizing,
                data.symbol,
                v["random_baseline_runs"],
                v["seed"],
                int(frame.index.searchsorted(start)),
            ),
        },
        "series": {
            "equity": _downsample(full_e),
            "drawdown_pct": _downsample(M.drawdown_series(full_e)),
            "split": str(split),
        },
        "trades_sample": [_trade_row(t) for t in full_t if t.filled][-200:],
    }


def _downsample(series: pd.Series, points: int = 600) -> list[list]:
    s = series.dropna()
    if len(s) > points:
        s = s.iloc[:: max(1, len(s) // points)]
    return [[ts.isoformat(), round(float(v), 4)] for ts, v in s.items()]


def _trade_row(t: Trade) -> dict[str, Any]:
    return {
        "signal_time": str(t.signal_time),
        "side": t.side,
        "entry": round(t.entry_price, 6) if t.entry_price else None,
        "exit": round(t.exit_price, 6) if t.exit_price else None,
        "reason": t.exit_reason,
        "signal_r": round(t.signal_r, 3),
        "realized_r": round(t.realized_r, 3),
        "net_pnl": round(t.net_pnl, 2),
        "partial": t.partial,
    }


def evaluate(cfg: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
    v = _validation(cfg)
    oos_trades = sum(r["performance"]["out_of_sample"]["trades"] for r in results)
    weighted = [
        (
            r["performance"]["out_of_sample"]["expectancy_r"],
            r["performance"]["out_of_sample"]["trades"],
        )
        for r in results
        if r["performance"]["out_of_sample"]["trades"]
    ]
    oos_exp = sum(e * n for e, n in weighted) / sum(n for _, n in weighted) if weighted else None
    wf = [r["walk_forward"] for r in results if r["walk_forward"]["expectancy_r"] is not None]
    wf_exp = (
        sum(w["expectancy_r"] * w["trades"] for w in wf) / sum(w["trades"] for w in wf)
        if wf
        else None
    )
    pos_share = [w["positive_window_share"] for w in wf if w["positive_window_share"] is not None]
    be = [r["cost_sensitivity"]["break_even_multiplier"] for r in results]
    be_min = min((b for b in be if b is not None), default=None)
    rnd = [r["baselines"]["random_entries"] for r in results if r["baselines"]["random_entries"]]
    beats = [b["strategy_expectancy_r"] > b["p95_expectancy_r"] for b in rnd]
    criteria = [
        {
            "name": f"At least {v['min_oos_trades']} out-of-sample trades",
            "value": oos_trades,
            "passed": oos_trades >= v["min_oos_trades"],
        },
        {
            "name": "Out-of-sample expectancy after costs is positive",
            "value": None if oos_exp is None else round(oos_exp, 4),
            "passed": oos_exp is not None and oos_exp > 0,
        },
        {
            "name": "Walk-forward expectancy after costs is positive",
            "value": None if wf_exp is None else round(wf_exp, 4),
            "passed": wf_exp is not None and wf_exp > 0,
        },
        {
            "name": f"At least {int(v['min_positive_wf_windows'] * 100)}% of walk-forward windows positive",
            "value": None if not pos_share else round(float(np.mean(pos_share)), 3),
            "passed": bool(pos_share) and float(np.mean(pos_share)) >= v["min_positive_wf_windows"],
        },
        {
            "name": f"Still profitable at {v['min_break_even_cost_multiplier']}x costs on every symbol",
            "value": be_min,
            "passed": be_min is not None and be_min >= v["min_break_even_cost_multiplier"],
        },
        {
            "name": "Beats the 95th percentile of random entries with the same geometry",
            "value": f"{sum(beats)}/{len(beats)} symbols" if beats else None,
            "passed": bool(beats) and all(beats),
        },
    ]
    passed = all(c["passed"] for c in criteria)
    return {
        "status": "validated_candidate" if passed else "not_validated",
        "criteria": criteria,
        "note": (
            "A strategy is not called successful because of a high historical win rate. "
            "It must survive out-of-sample and walk-forward testing after realistic costs, "
            "keep an edge at higher costs, and beat random entries with the same risk geometry."
        ),
    }


def run_experiment(
    cfg: dict[str, Any], datasets: list[Dataset], *, settings_fingerprint: str | None = None
) -> dict[str, Any]:
    results = []
    for ds in datasets:
        logger.info("Running %s on %s", cfg.get("name"), ds.symbol)
        results.append(run_symbol(cfg, ds))
    validation = evaluate(cfg, results)
    pooled_oos = _pool([r["performance"]["out_of_sample"] for r in results])
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "kind": "backtest",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strategy": {
            "name": cfg.get("name"),
            "version": cfg.get("version"),
            "description": cfg.get("description"),
            "config_path": cfg.get("_path"),
            "config_hash": cfg.get("_hash"),
            "params": build(cfg).params(),
            "symbols": [d.symbol for d in datasets],
            "timeframe": results[0]["timeframe"] if results else None,
        },
        "data": [_data_section(d) for d in datasets],
        "costs": _costs(cfg).to_dict(),
        "sizing": _sizing(cfg).__dict__,
        "results": results,
        "summary": {"out_of_sample_pooled": pooled_oos},
        "validation": validation,
        "reliability": {
            "applicable": False,
            "note": "Historical simulation: outages, missed events and recovery times are measured "
            "in paper/live session reports (signal-bot-research session-report).",
        },
        "provenance": provenance(
            settings_fingerprint,
            {
                "name": cfg.get("name"),
                "version": cfg.get("version"),
                "config_hash": cfg.get("_hash"),
            },
        )
        | {"seed": _validation(cfg)["seed"], "deterministic": True},
    }
    report["report_id"] = hashlib.sha256(
        json.dumps(
            {
                "strategy": report["strategy"],
                "data": [d["snapshot_id"] for d in report["data"]],
                "costs": report["costs"],
                "code": report["provenance"]["code"]["commit"],
            },
            sort_keys=True,
            default=str,
        ).encode()
    ).hexdigest()[:16]
    return report


def _data_section(d: Dataset) -> dict[str, Any]:
    q = d.quality()
    return {
        "symbol": d.symbol,
        "source": d.source,
        "synthetic": d.synthetic,
        "snapshot_id": d.snapshot_id,
        "quality": {k: v for k, v in q.items() if k != "gaps"},
        "gaps": q["gaps"][:10],
    }


def _pool(perfs: list[dict[str, Any]]) -> dict[str, Any]:
    n = sum(p["trades"] for p in perfs)
    if not n:
        return {"trades": 0}
    exp = sum((p["expectancy_r"] or 0) * p["trades"] for p in perfs) / n
    win = sum((p["win_rate"] or 0) * p["trades"] for p in perfs) / n
    return {
        "trades": n,
        "expectancy_r": round(exp, 4),
        "win_rate": round(win, 4),
        "net_return_pct_by_symbol": [p["net_return_pct"] for p in perfs],
        "worst_drawdown_pct": min(p["max_drawdown_pct"] for p in perfs),
    }


def deterministic_view(report: dict[str, Any]) -> dict[str, Any]:
    """The report minus fields that legitimately differ between identical runs."""
    clean = json.loads(json.dumps(report, default=str))
    clean.pop("generated_at", None)
    clean.get("provenance", {}).pop("runtime", None)
    return clean


__all__ = ["deterministic_view", "load_config", "run_experiment"]
