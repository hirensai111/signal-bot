"""Trend-breakout decision core shared by research and live trading.

``trend_decisions`` is the *only* implementation of the strategy's entry rule.
The research backtest (``research.strategies.TrendBreakout``) calls it on a full
history; the live generator (``signals.trend_live``) calls it on the history of
completed 4h candles it has seen so far and keeps only the decision for the
newest bar.  Every indicator is causal and computed left to right, so the value
at bar *i* is the same whether or not later bars exist.  Live and backtest
decisions are therefore identical whenever both start from the same first bar
(see ``tests/test_trend_live.py``).

Indicator definitions (unchanged from the published research):

* ATR: Wilder smoothing of the true range, ``ewm(alpha=1/n, adjust=False)``.
* Donchian channel: highest high / lowest low of the previous ``entry_lookback`` bars.
* Trend filter: ``ewm(span=trend_ema, adjust=False)`` of the close.
* Volume filter: the bar's volume >= ``breakout_volume_mult`` x the mean of the previous 20 bars.

Exits are the fill model's / the execution engine's job; the decision only carries the
levels: the initial stop (``stop_atr`` x ATR), the trailing distance (``trail_atr`` x ATR,
0 = no trailing) and an optional fixed take-profit ``take_profit_r`` x the initial risk
from the entry (0 = no target).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Any

import numpy as np
import pandas as pd

VOLUME_MEAN_BARS = 20


@dataclass(frozen=True)
class TrendParams:
    timeframe: str = "4h"
    entry_lookback: int = 55
    atr_n: int = 20
    stop_atr: float = 2.0
    trail_atr: float = 3.0
    trend_ema: int = 0  # 0 = off; otherwise only trade in the EMA's direction
    breakout_volume_mult: float = 0.0  # 0 = off
    allow_short: bool = True
    max_hold_bars: int = 180
    min_stop_bps: float = 0.0
    take_profit_r: float = 0.0  # 0 = no fixed target; otherwise entry +/- this many R

    @classmethod
    def from_mapping(cls, params: dict[str, Any]) -> TrendParams:
        names = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in params.items() if k in names})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def warmup_bars(self) -> int:
        """Bars needed before the first decision can be made at all."""
        return max(self.entry_lookback + 1, self.atr_n, VOLUME_MEAN_BARS + 1)


@dataclass(frozen=True)
class TrendDecision:
    """An entry decision taken at the close of bar ``bar``."""

    bar: int
    time: pd.Timestamp  # open time of the decision bar
    side: str  # "long" | "short"
    entry: float  # the decision bar's close
    stop: float
    trail_distance: float
    atr: float
    channel: float  # the broken Donchian level
    ema: float | None
    volume_ratio: float | None
    max_hold_bars: int
    take_profit: float | None = None  # fixed target price, None = no target

    @property
    def strength(self) -> float:
        """Breakout distance beyond the channel in ATRs (used to rank simultaneous entries)."""
        if self.atr <= 0:
            return 0.0
        beyond = self.entry - self.channel if self.side == "long" else self.channel - self.entry
        return beyond / self.atr


def atr(frame: pd.DataFrame, n: int = 14) -> pd.Series:
    prev = frame["close"].shift(1)
    tr = pd.concat(
        [frame["high"] - frame["low"], (frame["high"] - prev).abs(), (frame["low"] - prev).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()


def trend_decisions(frame: pd.DataFrame, p: TrendParams, start: int = 0) -> list[TrendDecision]:
    """Entry decisions for bars ``start..len(frame)-1`` of *frame* (bars on ``p.timeframe``)."""
    if len(frame) == 0:
        return []
    a = atr(frame, p.atr_n).to_numpy()
    c = frame["close"].to_numpy(float)
    upper = frame["high"].rolling(p.entry_lookback).max().shift(1).to_numpy()
    lower = frame["low"].rolling(p.entry_lookback).min().shift(1).to_numpy()
    ema = (
        frame["close"].ewm(span=p.trend_ema, adjust=False).mean().to_numpy()
        if p.trend_ema
        else None
    )
    vol = frame["volume"].to_numpy(float)
    vol_mean = frame["volume"].rolling(VOLUME_MEAN_BARS).mean().shift(1).to_numpy()
    idx = frame.index
    out: list[TrendDecision] = []
    for i in range(max(start, p.entry_lookback + 1), len(frame)):
        if not (np.isfinite(a[i]) and np.isfinite(upper[i]) and np.isfinite(lower[i])):
            continue
        if p.breakout_volume_mult and not (
            np.isfinite(vol_mean[i]) and vol[i] >= p.breakout_volume_mult * vol_mean[i]
        ):
            continue
        risk = p.stop_atr * a[i]
        if risk <= 0 or risk / c[i] * 1e4 < p.min_stop_bps:
            continue
        up_ok = ema is None or c[i] > ema[i]
        down_ok = ema is None or c[i] < ema[i]
        if c[i] > upper[i] and up_ok:
            side, stop, channel = "long", c[i] - risk, upper[i]
        elif p.allow_short and c[i] < lower[i] and down_ok:
            side, stop, channel = "short", c[i] + risk, lower[i]
        else:
            continue
        target = None
        if p.take_profit_r > 0:
            target = (
                c[i] + p.take_profit_r * risk if side == "long" else c[i] - p.take_profit_r * risk
            )
        out.append(
            TrendDecision(
                bar=i,
                time=idx[i],
                side=side,
                entry=float(c[i]),
                stop=float(stop),
                trail_distance=float(p.trail_atr * a[i]),
                atr=float(a[i]),
                channel=float(channel),
                ema=None if ema is None else float(ema[i]),
                volume_ratio=float(vol[i] / vol_mean[i])
                if np.isfinite(vol_mean[i]) and vol_mean[i] > 0
                else None,
                max_hold_bars=p.max_hold_bars,
                take_profit=None if target is None else float(target),
            )
        )
    return out


def trail_stop(side: str, stop: float, best: float, trail_distance: float) -> float:
    """The ratchet both the fill model and the live trailing manager apply: never loosens."""
    if side == "long":
        return max(stop, best - trail_distance)
    return min(stop, best + trail_distance)
