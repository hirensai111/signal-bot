"""Strategies under research.  Each emits :class:`Intent` objects causally
(the decision at bar *i* uses only bars ``<= i``).

* :class:`LiquiditySweepBaseline` — the production strategy.  It does **not**
  re-implement the rules: it replays candles through the live HTF scanner,
  microstructure engine, sweep detector and risk sizer (in-memory SQLite), so
  research and production cannot drift apart.
* :class:`ExhaustionReversion` — fades climactic runs (N same-direction bars
  covering k·ATR with a volume spike).
* :class:`RegimeFilter` — wraps any strategy and vetoes trades in regimes
  where it is not expected to work (dead volatility, disorderly volatility,
  strong trends for reversion).
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import pandas as pd

from signal_bot.research.data import INTERVAL_MINUTES, Dataset, resample
from signal_bot.research.fill_model import Intent

logger = logging.getLogger(__name__)


class Strategy(Protocol):
    name: str
    version: str
    timeframe: str

    def params(self) -> dict[str, Any]: ...

    def generate(self, data: Dataset) -> list[Intent]:
        """*data* is the base dataset (1m); intents index the ``timeframe`` frame."""
        ...


def atr(frame: pd.DataFrame, n: int = 14) -> pd.Series:
    prev = frame["close"].shift(1)
    tr = pd.concat(
        [frame["high"] - frame["low"], (frame["high"] - prev).abs(), (frame["low"] - prev).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()


def frame_for(data: Dataset, timeframe: str) -> pd.DataFrame:
    if data.interval == timeframe:
        return data.frame
    if INTERVAL_MINUTES[timeframe] < INTERVAL_MINUTES[data.interval]:
        raise ValueError(f"cannot build {timeframe} bars from {data.interval} data")
    return resample(data.frame, timeframe)


# ---------------------------------------------------------------------------
# exhaustion reversion
# ---------------------------------------------------------------------------


@dataclass
class ExhaustionReversion:
    timeframe: str = "5m"
    run_bars: int = 4
    run_atr: float = 2.5
    climax_vol_z: float = 2.0
    vol_window: int = 50
    stop_atr_buffer: float = 0.25
    min_stop_bps: float = 0.0  # skip setups whose stop is too tight to pay for round-trip costs
    reward_risk: float = 1.5
    max_hold_bars: int = 24
    name: str = "exhaustion_reversion"
    version: str = "0.1.0"

    def params(self) -> dict[str, Any]:
        return {
            k: getattr(self, k)
            for k in (
                "timeframe",
                "run_bars",
                "run_atr",
                "climax_vol_z",
                "vol_window",
                "stop_atr_buffer",
                "min_stop_bps",
                "reward_risk",
                "max_hold_bars",
            )
        }

    def generate(self, data: Dataset) -> list[Intent]:
        f = frame_for(data, self.timeframe)
        a = atr(f).to_numpy()
        o, h, lo, c = (f[k].to_numpy(float) for k in ("open", "high", "low", "close"))
        vol = f["volume"]
        vz = (
            (vol - vol.rolling(self.vol_window).mean()) / vol.rolling(self.vol_window).std()
        ).to_numpy()
        up = c > o
        down = c < o
        n = self.run_bars
        intents: list[Intent] = []
        cooldown_until = -1
        for i in range(max(n, self.vol_window), len(f)):
            if i <= cooldown_until or not np.isfinite(a[i]) or not np.isfinite(vz[i]):
                continue
            run = slice(i - n + 1, i + 1)
            move = c[i] - o[i - n + 1]
            if vz[i] < self.climax_vol_z or abs(move) < self.run_atr * a[i]:
                continue
            if down[run].all() and move < 0:
                stop = lo[run].min() - self.stop_atr_buffer * a[i]
                entry = c[i]
                if entry - stop <= 0 or (entry - stop) / entry * 1e4 < self.min_stop_bps:
                    continue
                intents.append(
                    Intent(
                        i,
                        "long",
                        entry,
                        stop,
                        entry + self.reward_risk * (entry - stop),
                        self.max_hold_bars,
                        "exhaustion",
                    )
                )
                cooldown_until = i + n
            elif up[run].all() and move > 0:
                stop = h[run].max() + self.stop_atr_buffer * a[i]
                entry = c[i]
                if stop - entry <= 0 or (stop - entry) / entry * 1e4 < self.min_stop_bps:
                    continue
                intents.append(
                    Intent(
                        i,
                        "short",
                        entry,
                        stop,
                        entry - self.reward_risk * (stop - entry),
                        self.max_hold_bars,
                        "exhaustion",
                    )
                )
                cooldown_until = i + n
        return intents


# ---------------------------------------------------------------------------
# regime filter
# ---------------------------------------------------------------------------


@dataclass
class RegimeFilter:
    inner: Any
    min_vol_pct: float = 0.20
    max_vol_pct: float = 0.97
    max_trend_er: float | None = 0.5
    er_window: int = 48
    rank_window: int = 2000
    stats: dict[str, int] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return f"{self.inner.name}+regime_filter"

    @property
    def version(self) -> str:
        return self.inner.version

    @property
    def timeframe(self) -> str:
        return self.inner.timeframe

    def params(self) -> dict[str, Any]:
        return {
            **self.inner.params(),
            "regime_filter": {
                "min_vol_pct": self.min_vol_pct,
                "max_vol_pct": self.max_vol_pct,
                "max_trend_er": self.max_trend_er,
                "er_window": self.er_window,
                "rank_window": self.rank_window,
            },
        }

    def regimes(self, f: pd.DataFrame) -> pd.DataFrame:
        atr_pct = atr(f) / f["close"]
        rank = atr_pct.rolling(self.rank_window, min_periods=200).rank(pct=True)
        change = (f["close"] - f["close"].shift(self.er_window)).abs()
        path = f["close"].diff().abs().rolling(self.er_window).sum()
        er = change / path
        return pd.DataFrame({"vol_rank": rank, "efficiency": er})

    def generate(self, data: Dataset) -> list[Intent]:
        intents = self.inner.generate(data)
        f = frame_for(data, self.timeframe)
        reg = self.regimes(f)
        kept, reasons = [], {"low_volatility": 0, "high_volatility": 0, "trending": 0, "warmup": 0}
        for it in intents:
            row = reg.iloc[it.bar]
            if not np.isfinite(row.vol_rank) or not np.isfinite(row.efficiency):
                reasons["warmup"] += 1
                continue
            if row.vol_rank < self.min_vol_pct:
                reasons["low_volatility"] += 1
            elif row.vol_rank > self.max_vol_pct:
                reasons["high_volatility"] += 1
            elif self.max_trend_er is not None and row.efficiency > self.max_trend_er:
                reasons["trending"] += 1
            else:
                kept.append(it)
        self.stats = {"candidates": len(intents), "kept": len(kept), **reasons}
        return kept


# ---------------------------------------------------------------------------
# liquidity sweep baseline (production code path)
# ---------------------------------------------------------------------------


@dataclass
class LiquiditySweepBaseline:
    timeframe: str = "5m"
    # The live engine has no time stop: positions run until their SL/TP fill.  Research
    # mirrors that (capped at 7 days so a run always terminates).  Set hold_multiple to
    # study a time stop at est_hold_minutes * hold_multiple instead.
    hold_multiple: int | None = None
    max_hold_days: int = 7
    cache_dir: str = "data/cache"
    name: str = "liquidity_sweep"
    version: str = "1.0.0"

    def params(self) -> dict[str, Any]:
        from signal_bot.signals.sweep_detector import SweepDetector

        return {
            "timeframe": self.timeframe,
            "htf_lookback": 3,
            "cooldown_candles": SweepDetector.COOLDOWN_CANDLES,
            "min_rr": str(SweepDetector.MIN_RR),
            "vol_zscore_min": 2.0,
            "vol_ratio_min": 2.5,
            "time_stop": (
                f"est_hold_minutes x {self.hold_multiple}"
                if self.hold_multiple
                else f"none (live behaviour; {self.max_hold_days}d cap)"
            ),
            "implementation": "live modules replayed (signal_bot.backtest.runner)",
        }

    def generate(self, data: Dataset) -> list[Intent]:
        if data.interval != "1m":
            raise ValueError("the sweep baseline replays 1m data (CVD is built from 1m taker flow)")
        f = frame_for(data, self.timeframe)
        signals = self._cached_signals(data)
        pos = {ts: i for i, ts in enumerate(f.index)}
        if self.hold_multiple:
            hold_minutes = {"1m": 30, "5m": 120}[self.timeframe] * self.hold_multiple
        else:
            hold_minutes = self.max_hold_days * 24 * 60
        hold_bars = hold_minutes // INTERVAL_MINUTES[self.timeframe]
        out = []
        for s in signals:
            i = pos.get(pd.Timestamp(s["open_time"]))
            if i is None:
                continue
            out.append(
                Intent(
                    i,
                    s["side"],
                    float(s["entry"]),
                    float(s["stop"]),
                    float(s["take_profit"]),
                    hold_bars,
                    "sweep",
                )
            )
        return out

    def _cached_signals(self, data: Dataset) -> list[dict]:
        """Replaying a year through the live modules takes minutes: cache by data snapshot."""
        from signal_bot.backtest.runner import replay_live_signals  # heavy: DB-backed modules

        key = f"{data.symbol}_{self.timeframe}_{data.snapshot_id}_v{self.version}"
        path = Path(self.cache_dir) / "sweep_signals" / f"{key}.json"
        if path.exists():
            raw = json.loads(path.read_text())
        else:
            raw = asyncio.run(replay_live_signals(data.symbol, data.frame, self.timeframe))
            raw = [
                {
                    **s,
                    "open_time": str(s["open_time"]),
                    **{k: str(s[k]) for k in ("entry", "stop", "take_profit")},
                }
                for s in raw
            ]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(raw, default=str))
        return raw


# ---------------------------------------------------------------------------
# trend breakout (higher timeframe, exchange-native trailing stop)
# ---------------------------------------------------------------------------


@dataclass
class TrendBreakout:
    """Donchian-channel breakout with an ATR stop and a constant-distance trailing stop.

    Long when the close breaks above the highest high of the previous ``entry_lookback``
    bars (short below the lowest low).  Initial stop ``stop_atr`` x ATR away; the stop
    then trails the best price by ``trail_atr`` x ATR (distance fixed at entry), with no
    fixed target.  The trail is *stepped*: it moves once per completed bar, from the
    highs/lows of bars already closed, which is what the live trailing-stop manager does
    (an exchange-native trail such as Delta's ``bracket_trail_amount`` moves continuously
    and is a different exit).  Optional EMA trend filter and breakout-volume filter.
    The decision rule is ``signals.trend.trend_decisions``, shared with live trading.
    """

    timeframe: str = "4h"
    entry_lookback: int = 55
    atr_n: int = 20
    stop_atr: float = 2.0
    trail_atr: float = 3.0
    trend_ema: int = 0  # 0 = off; otherwise only trade in the EMA's direction
    breakout_volume_mult: float = 0.0  # 0 = off; breakout bar volume >= mult x 20-bar mean
    allow_short: bool = True
    max_hold_bars: int = 180
    min_stop_bps: float = 0.0
    name: str = "trend_breakout"
    version: str = "0.1.0"

    def params(self) -> dict[str, Any]:
        return {
            k: getattr(self, k)
            for k in (
                "timeframe",
                "entry_lookback",
                "atr_n",
                "stop_atr",
                "trail_atr",
                "trend_ema",
                "breakout_volume_mult",
                "allow_short",
                "max_hold_bars",
                "min_stop_bps",
            )
        }

    def generate(self, data: Dataset) -> list[Intent]:
        from signal_bot.research.fill_model import NO_TARGET
        from signal_bot.signals.trend import TrendParams, trend_decisions

        # The decision rule lives in signals/trend.py so live trading runs the same code.
        decisions = trend_decisions(
            frame_for(data, self.timeframe), TrendParams.from_mapping(self.params())
        )
        return [
            Intent(
                d.bar,
                d.side,
                d.entry,
                d.stop,
                NO_TARGET if d.side == "long" else -NO_TARGET,
                d.max_hold_bars,
                "breakout",
                d.trail_distance,
            )
            for d in decisions
        ]


# ---------------------------------------------------------------------------
# liquidity sweep v2: the live signals with cost-aware trade management
# ---------------------------------------------------------------------------


@dataclass
class LiquiditySweepV2:
    """The production sweep signals with the management the v1 diagnosis called for.

    v1 lost because ~80% of trades were stopped out against targets 3.6-4.8R away, and
    a third of stops were tighter than ~2.5x round-trip costs.  v2 keeps the live entry
    and stop, skips setups whose stop is under ``min_stop_bps``, and replaces the
    level-based target with a fixed ``target_r`` (or a trailing stop when
    ``trail_r`` > 0).
    """

    timeframe: str = "5m"
    min_stop_bps: float = 40.0
    target_r: float = 2.0
    trail_r: float = 0.0
    max_hold_days: int = 7
    cache_dir: str = "data/cache"
    name: str = "liquidity_sweep_v2"
    version: str = "0.1.0"

    def params(self) -> dict[str, Any]:
        return {
            "timeframe": self.timeframe,
            "entry_and_stop": "live sweep detector (unchanged)",
            "min_stop_bps": self.min_stop_bps,
            "target_r": self.target_r,
            "trail_r": self.trail_r,
            "max_hold_days": self.max_hold_days,
        }

    def generate(self, data: Dataset) -> list[Intent]:
        from signal_bot.research.fill_model import NO_TARGET

        base = LiquiditySweepBaseline(
            timeframe=self.timeframe, max_hold_days=self.max_hold_days, cache_dir=self.cache_dir
        )
        out = []
        for it in base.generate(data):
            risk = abs(it.entry - it.stop)
            if risk <= 0 or risk / it.entry * 1e4 < self.min_stop_bps:
                continue
            sign = 1 if it.side == "long" else -1
            if self.trail_r > 0:
                tp = NO_TARGET * sign
                trail = self.trail_r * risk
            else:
                tp = it.entry + sign * self.target_r * risk
                trail = None
            out.append(
                Intent(it.bar, it.side, it.entry, it.stop, tp, it.max_hold_bars, "sweep_v2", trail)
            )
        return out


REGISTRY = {
    "exhaustion_reversion": ExhaustionReversion,
    "liquidity_sweep": LiquiditySweepBaseline,
    "liquidity_sweep_v2": LiquiditySweepV2,
    "trend_breakout": TrendBreakout,
}


def build(config: dict) -> Any:
    cls = REGISTRY[config["strategy"]]
    params = dict(config.get("params", {}))
    strategy = cls(**params)
    strategy.version = config.get("version", strategy.version)
    rf = config.get("regime_filter")
    if rf and rf.get("enabled", True):
        rf = {k: v for k, v in rf.items() if k != "enabled"}
        strategy = RegimeFilter(strategy, **rf)
    return strategy
