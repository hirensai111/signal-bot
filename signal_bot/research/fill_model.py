"""Deterministic, price- and liquidity-informed execution simulator for bar data.

Replaces the old backtester's ``random() < no_fill_rate``.  Every outcome is a
function of the bars, so a run is exactly reproducible.

Entry (decided at the close of bar *i*, executed at bar *i+1*):

* the order is a marketable limit at ``entry ± max_entry_slippage_bps``; if the
  next bar *opens* beyond that limit (a gap), the order does not fill;
* fill price = next open ± half spread ± square-root market impact scaled by
  the bar's own volatility (range as a sigma proxy):
  ``impact_bps = impact_coef * bar_range_bps * sqrt(order_notional / bar_notional)``;
* quantity is capped at ``max_participation`` of the bar's traded notional, so
  large orders in thin bars are partially filled.

Exits, evaluated from the entry bar onwards:

* stop: stop-market; if the bar opens through the stop it fills at the open
  (gap risk), otherwise at the stop; then pays spread + impact + taker fee;
* take-profit: resting limit, fills at its price (maker fee), or at the open if
  the bar opens through it;
* both touched in one bar: the stop is assumed first (conservative);
* time stop after ``max_hold_bars`` at the close (taker);
* funding is charged per 8h boundary crossed while open.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field, replace

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class CostModel:
    taker_fee: float = 0.0005
    maker_fee: float = 0.0002
    half_spread_bps: float = 1.0
    impact_coef: float = 0.5
    max_participation: float = 0.05
    max_entry_slippage_bps: float = 15.0
    funding_rate_8h: float = 0.0001  # longs pay when positive

    def scaled(self, k: float) -> CostModel:
        """All frictions multiplied by *k* (for cost-sensitivity analysis)."""
        return replace(
            self,
            taker_fee=self.taker_fee * k,
            maker_fee=self.maker_fee * k,
            half_spread_bps=self.half_spread_bps * k,
            impact_coef=self.impact_coef * k,
            funding_rate_8h=self.funding_rate_8h * k,
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Sizing:
    initial_equity: float = 10_000.0
    risk_fraction: float = 0.005  # 0.5 % of current equity per trade
    max_leverage: float = 10.0


@dataclass(frozen=True)
class Intent:
    """A strategy's decision at the close of bar ``bar``."""

    bar: int
    side: str  # "long" | "short"
    entry: float
    stop: float
    take_profit: float
    max_hold_bars: int = 48
    tag: str = ""


@dataclass
class Trade:
    symbol: str
    side: str
    signal_time: pd.Timestamp
    entry_time: pd.Timestamp | None
    exit_time: pd.Timestamp | None
    intended_entry: float
    entry_price: float | None
    stop: float
    take_profit: float
    exit_price: float | None = None
    exit_reason: str = "no_fill"
    qty_requested: float = 0.0
    qty_filled: float = 0.0
    fees: float = 0.0
    spread_cost: float = 0.0
    impact_cost: float = 0.0
    funding: float = 0.0
    gross_pnl: float = 0.0
    net_pnl: float = 0.0
    risk_amount: float = 0.0
    signal_r: float = 0.0
    realized_r: float = 0.0
    equity_after: float = 0.0
    tag: str = ""
    filled: bool = False
    partial: bool = False
    extra: dict = field(default_factory=dict)


def _impact_bps(cost: CostModel, notional: float, bar_notional: float, range_bps: float) -> float:
    """Square-root impact: k * sigma * sqrt(participation), sigma ~ the bar's range in bps."""
    if bar_notional <= 0:
        return cost.impact_coef * range_bps
    return cost.impact_coef * range_bps * math.sqrt(min(1.0, max(notional, 0.0) / bar_notional))


def simulate(
    frame: pd.DataFrame,
    intents: list[Intent],
    cost: CostModel,
    sizing: Sizing,
    symbol: str = "",
) -> tuple[list[Trade], pd.Series]:
    """Run intents through the fill model.  One position per symbol at a time."""
    o = frame["open"].to_numpy(float)
    h = frame["high"].to_numpy(float)
    lo = frame["low"].to_numpy(float)
    c = frame["close"].to_numpy(float)
    qv = frame["quote_volume"].to_numpy(float)
    rng_bps = np.where(c > 0, (h - lo) / np.where(c > 0, c, 1) * 1e4, 0.0)
    idx = frame.index
    n = len(frame)
    equity = sizing.initial_equity
    curve = np.full(n, np.nan)
    trades: list[Trade] = []
    busy_until = -1
    by_bar: dict[int, Intent] = {}
    for it in sorted(intents, key=lambda x: x.bar):
        by_bar.setdefault(it.bar, it)

    for i in range(n):
        it = by_bar.get(i)
        if it is None or i <= busy_until or i + 1 >= n:
            curve[i] = equity
            continue
        long = it.side == "long"
        sign = 1.0 if long else -1.0
        risk_per_unit = abs(it.entry - it.stop)
        trade = Trade(
            symbol=symbol,
            side=it.side,
            signal_time=idx[i],
            entry_time=None,
            exit_time=None,
            intended_entry=it.entry,
            entry_price=None,
            stop=it.stop,
            take_profit=it.take_profit,
            tag=it.tag,
        )
        if risk_per_unit <= 0:
            trade.exit_reason = "invalid"
            trades.append(trade)
            curve[i] = equity
            continue

        risk_amount = equity * sizing.risk_fraction
        qty = min(risk_amount / risk_per_unit, equity * sizing.max_leverage / it.entry)
        trade.qty_requested = qty
        j = i + 1
        limit = it.entry * (1 + sign * cost.max_entry_slippage_bps / 1e4)
        if (long and o[j] > limit) or (not long and o[j] < limit):
            trade.exit_reason = "no_fill_gap"
            trades.append(trade)
            curve[i] = equity
            continue
        cap_qty = cost.max_participation * qv[j] / o[j] if o[j] > 0 else 0.0
        fill_qty = min(qty, cap_qty)
        if fill_qty <= 0:
            trade.exit_reason = "no_fill_liquidity"
            trades.append(trade)
            curve[i] = equity
            continue
        notional = fill_qty * o[j]
        spread = cost.half_spread_bps / 1e4 * o[j]
        impact = _impact_bps(cost, notional, qv[j], rng_bps[j]) / 1e4 * o[j]
        entry_px = o[j] + sign * (spread + impact)
        trade.entry_time = idx[j]
        trade.entry_price = entry_px
        trade.qty_filled = fill_qty
        trade.partial = fill_qty < qty * 0.999
        trade.filled = True
        trade.risk_amount = fill_qty * risk_per_unit
        fees = entry_px * fill_qty * cost.taker_fee
        spread_cost = spread * fill_qty
        impact_cost = impact * fill_qty

        exit_px = None
        reason = "time"
        k = j
        last = min(n - 1, j + it.max_hold_bars)
        funding = 0.0
        while k <= last:
            if k > j and _crosses_funding(idx[k - 1], idx[k]):
                funding -= sign * fill_qty * o[k] * cost.funding_rate_8h
            stop_hit = (lo[k] <= it.stop) if long else (h[k] >= it.stop)
            tp_hit = (h[k] >= it.take_profit) if long else (lo[k] <= it.take_profit)
            if stop_hit:
                gap = (o[k] <= it.stop) if long else (o[k] >= it.stop)
                base = o[k] if (gap and k > j) else it.stop
                if k == j and ((long and o[k] <= it.stop) or (not long and o[k] >= it.stop)):
                    base = o[k]
                sp = cost.half_spread_bps / 1e4 * base
                im = _impact_bps(cost, fill_qty * base, qv[k], rng_bps[k]) / 1e4 * base
                exit_px = base - sign * (sp + im)
                fees += exit_px * fill_qty * cost.taker_fee
                spread_cost += sp * fill_qty
                impact_cost += im * fill_qty
                reason = "sl"
                break
            if tp_hit:
                through = (o[k] >= it.take_profit) if long else (o[k] <= it.take_profit)
                exit_px = o[k] if (through and k > j) else it.take_profit
                fees += exit_px * fill_qty * cost.maker_fee
                reason = "tp"
                break
            k += 1
        if exit_px is None:
            k = last
            base = c[k]
            sp = cost.half_spread_bps / 1e4 * base
            im = _impact_bps(cost, fill_qty * base, qv[k], rng_bps[k]) / 1e4 * base
            exit_px = base - sign * (sp + im)
            fees += exit_px * fill_qty * cost.taker_fee
            spread_cost += sp * fill_qty
            impact_cost += im * fill_qty
            reason = "time"

        equity_before = equity
        gross = sign * (exit_px - entry_px) * fill_qty
        net = gross - fees + funding
        level = it.stop if reason == "sl" else it.take_profit if reason == "tp" else c[k]
        trade.exit_time = idx[k]
        trade.exit_price = exit_px
        trade.exit_reason = reason
        trade.fees = fees
        trade.spread_cost = spread_cost
        trade.impact_cost = impact_cost
        trade.funding = funding
        trade.gross_pnl = gross
        trade.net_pnl = net
        trade.signal_r = sign * (level - it.entry) / risk_per_unit
        trade.realized_r = net / (qty * risk_per_unit)
        equity += net
        trade.equity_after = equity
        trades.append(trade)
        curve[i:k] = equity_before  # realised-equity curve: PnL books on exit
        curve[k] = equity
        busy_until = k

    series = pd.Series(curve, index=idx).ffill().fillna(sizing.initial_equity)
    return trades, series


def _crosses_funding(prev: pd.Timestamp, cur: pd.Timestamp) -> bool:
    return (cur.hour // 8 != prev.hour // 8) or (cur.date() != prev.date())


def trades_frame(trades: list[Trade]) -> pd.DataFrame:
    return pd.DataFrame([asdict(t) for t in trades])
