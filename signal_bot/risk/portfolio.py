"""Portfolio-level risk: one pool of capital shared by every symbol.

Eleven single-symbol backtests, each with its own $10k, are not one executable
portfolio.  This module decides, for a batch of entry candidates that arrive at
the same bar close, which ones the shared account can take and how big each is.
It is pure and deterministic, and it is used unchanged by:

* the live trend pipeline (``signals.trend_live``) and the risk governor's
  pre-submission re-check, with equity, open positions and stops from the venue;
* the research portfolio simulator (``research.portfolio``), with the same
  inputs reconstructed bar by bar from history.

Rules, applied to candidates in rank order (strongest breakout first, then symbol):

1. **Aggregate drawdown.** Drawdown from the equity peak at or beyond
   ``drawdown_halt`` refuses every new entry; at or beyond ``drawdown_reduce``
   the per-trade risk is multiplied by ``drawdown_risk_scale``.
2. **Sizing on shared equity.** Risk per trade is ``risk_per_trade`` x current
   equity (marked to market), converted to whole venue contracts (rounded down)
   and capped by the venue's position limit.  Fewer than one contract is refused.
3. **One position per symbol**, and at most ``max_positions`` open.
4. **Total open risk.** The sum over positions of what equity loses if every stop is
   hit from the current mark (trailed stops included), plus the candidate's risk, may
   not exceed ``max_open_risk``.
5. **Correlated risk.** With signed risks r (+ long, - short) and the correlation
   matrix C of recent returns, ``sqrt(r' C r)`` may not exceed ``max_correlated_risk``.
   Five longs in assets that move together count almost as one five-times-larger
   position; a long and a short in correlated assets partly offset.  Unknown
   correlations are taken as 1 (the conservative assumption for same-side risk).
6. **Gross leverage.** Total notional may not exceed ``max_gross_leverage`` x equity.
7. **Simultaneous entries.** At most ``max_entries_per_bar`` new entries per bar close.
8. **Margin safety.** Positions are opened at ``position_leverage`` (isolated margin);
   a stop farther than half the distance to liquidation (``0.5 / position_leverage``)
   is refused, so the stop, not the liquidation engine, closes a losing trade.  With
   ``max_gross_leverage`` 3 and ``position_leverage`` 5, margin in use stays <= 60%.

Every limit is a fraction of equity, so the rules scale with the account.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field, fields, replace
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PortfolioLimits:
    risk_per_trade: float = 0.005
    max_positions: int = 6
    max_open_risk: float = 0.03
    max_correlated_risk: float = 0.02
    max_gross_leverage: float = 3.0
    max_entries_per_bar: int = 3
    drawdown_reduce: float = 0.10
    drawdown_risk_scale: float = 0.5
    drawdown_halt: float = 0.20
    correlation_lookback_bars: int = 180  # 30 days of 4h bars
    correlation_min_bars: int = 60
    position_leverage: int = 5

    @classmethod
    def from_mapping(cls, cfg: Mapping[str, Any] | None) -> PortfolioLimits:
        names = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in (cfg or {}).items() if k in names})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContractSpec:
    """What the venue lets us trade: contract size and position limit (Delta products)."""

    contract_value: float  # base units per contract
    max_contracts: float | None = None  # venue position_size_limit


@dataclass(frozen=True)
class OpenPosition:
    symbol: str
    side: str  # "long" | "short"
    qty: float  # base units (contracts x contract value)
    entry: float
    mark: float
    stop: float

    @property
    def sign(self) -> float:
        return 1.0 if self.side == "long" else -1.0

    @property
    def risk(self) -> float:
        """What equity loses from the current mark if the stop is hit (never negative).

        Measured from the mark, not the entry: a winner whose trailed stop is far below
        the price still has that much equity at risk.
        """
        return max(0.0, self.sign * (self.mark - self.stop)) * self.qty

    @property
    def notional(self) -> float:
        return abs(self.qty * self.mark)


@dataclass(frozen=True)
class Candidate:
    symbol: str
    side: str
    entry: float
    stop: float
    strength: float = 0.0  # higher ranks first
    spec: ContractSpec = field(default_factory=lambda: ContractSpec(1.0))
    ref: Any = None  # caller's handle (e.g. the TrendDecision)
    # Re-checking an already-sized trade (governor, just before submission): keep its size.
    fixed_contracts: float | None = None


@dataclass(frozen=True)
class PortfolioState:
    equity: float
    peak_equity: float
    positions: tuple[OpenPosition, ...] = ()
    correlations: Mapping[tuple[str, str], float] = field(default_factory=dict)

    @property
    def drawdown(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, 1.0 - self.equity / self.peak_equity)


@dataclass(frozen=True)
class Admission:
    candidate: Candidate
    accepted: bool
    reason: str | None
    contracts: float = 0.0
    qty: float = 0.0  # base units
    risk_amount: float = 0.0
    risk_fraction: float = 0.0  # of equity, after any drawdown scaling
    notional: float = 0.0


def rank(candidates: Iterable[Candidate]) -> list[Candidate]:
    """Deterministic order for simultaneous entries: strongest breakout, then symbol."""
    return sorted(candidates, key=lambda c: (-round(c.strength, 12), c.symbol, c.side))


def correlation(state: PortfolioState, a: str, b: str) -> float:
    if a == b:
        return 1.0
    value = state.correlations.get((a, b), state.correlations.get((b, a)))
    if value is None or not math.isfinite(value):
        return 1.0
    return float(value)


def correlated_risk(state: PortfolioState, legs: list[tuple[str, float]]) -> float:
    """sqrt(r' C r) for signed risks *legs* = [(symbol, +/-risk), ...]."""
    if not legs:
        return 0.0
    total = 0.0
    for sa, ra in legs:
        for sb, rb in legs:
            total += ra * rb * correlation(state, sa, sb)
    return math.sqrt(max(total, 0.0))


def admit(
    state: PortfolioState, candidates: Iterable[Candidate], limits: PortfolioLimits
) -> list[Admission]:
    """Decide a batch of candidates that share one bar close.  Pure: no side effects."""
    ordered = rank(candidates)
    out: list[Admission] = []
    equity = state.equity
    if equity <= 0:
        return [Admission(c, False, "no_equity") for c in ordered]
    dd = state.drawdown
    eps = 1e-9  # 1 - 9000/10000 is 0.0999...98 in floating point
    if dd >= limits.drawdown_halt - eps:
        return [Admission(c, False, f"portfolio_drawdown_halt:{dd:.3f}") for c in ordered]
    risk_fraction = limits.risk_per_trade * (
        limits.drawdown_risk_scale if dd >= limits.drawdown_reduce - eps else 1.0
    )

    positions = list(state.positions)
    accepted_now = 0
    for c in ordered:
        reason = None
        per_unit = abs(c.entry - c.stop)
        contracts = qty = risk = notional = 0.0
        if per_unit <= 0 or c.entry <= 0:
            reason = "invalid_stop"
        elif any(p.symbol == c.symbol for p in positions):
            reason = "symbol_exposed"
        elif accepted_now >= limits.max_entries_per_bar:
            reason = "max_entries_per_bar"
        elif len(positions) >= limits.max_positions:
            reason = "max_positions"
        if reason is None and per_unit / c.entry >= 0.5 / max(limits.position_leverage, 1):
            reason = "stop_beyond_margin_buffer"
        if reason is None:
            cv = c.spec.contract_value
            if c.fixed_contracts is not None:
                contracts = float(c.fixed_contracts)
            else:
                target_qty = equity * risk_fraction / per_unit
                contracts = math.floor(target_qty / cv + 1e-9) if cv > 0 else 0.0
            if c.spec.max_contracts is not None:
                contracts = min(contracts, math.floor(c.spec.max_contracts))
            qty = contracts * cv
            risk = qty * per_unit
            notional = qty * c.entry
            if contracts < 1:
                reason = "below_min_contract"
        if reason is None:
            open_risk = sum(p.risk for p in positions)
            if open_risk + risk > limits.max_open_risk * equity * (1 + 1e-9):
                reason = "max_open_risk"
        if reason is None:
            sign = 1.0 if c.side == "long" else -1.0
            legs = [(p.symbol, p.sign * p.risk) for p in positions] + [(c.symbol, sign * risk)]
            if correlated_risk(state, legs) > limits.max_correlated_risk * equity * (1 + 1e-9):
                reason = "correlated_risk"
        if reason is None:
            gross = sum(p.notional for p in positions) + notional
            if gross > limits.max_gross_leverage * equity * (1 + 1e-9):
                reason = "max_gross_leverage"
        if reason is not None:
            out.append(Admission(c, False, reason, risk_fraction=risk_fraction))
            continue
        out.append(
            Admission(
                c,
                True,
                None,
                contracts=contracts,
                qty=qty,
                risk_amount=risk,
                risk_fraction=risk_fraction,
                notional=notional,
            )
        )
        positions.append(OpenPosition(c.symbol, c.side, qty, c.entry, c.entry, c.stop))
        accepted_now += 1
    return out


def exposure_summary(state: PortfolioState, limits: PortfolioLimits) -> dict[str, Any]:
    """Numbers the control room and the session report show."""
    equity = state.equity or 1.0
    legs = [(p.symbol, p.sign * p.risk) for p in state.positions]
    return {
        "equity": state.equity,
        "peak_equity": state.peak_equity,
        "drawdown_pct": round(state.drawdown * 100, 4),
        "positions": len(state.positions),
        "open_risk_pct": round(sum(p.risk for p in state.positions) / equity * 100, 4),
        "correlated_risk_pct": round(correlated_risk(state, legs) / equity * 100, 4),
        "gross_leverage": round(sum(p.notional for p in state.positions) / equity, 4),
        "net_long_notional": round(sum(p.sign * p.notional for p in state.positions), 2),
        "limits": limits.to_dict(),
        "entries_allowed": state.drawdown < limits.drawdown_halt - 1e-9,
        "risk_scaled": state.drawdown >= limits.drawdown_reduce - 1e-9,
    }


def log_returns(closes: Mapping[str, pd.Series]) -> pd.DataFrame:
    """Aligned (outer-joined) log returns, one column per symbol, oldest first."""
    cols = {}
    for s, series in closes.items():
        series = series.dropna()
        series = series[series > 0]
        cols[s] = np.log(series).diff()
    return pd.DataFrame(cols).sort_index()


def correlations_from_returns(window: pd.DataFrame, min_bars: int) -> dict[tuple[str, str], float]:
    """Pairwise Pearson correlation over *window* (pairs with < *min_bars* overlap omitted)."""
    corr = window.corr(min_periods=min_bars)
    out: dict[tuple[str, str], float] = {}
    names = sorted(window.columns)
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            value = corr.at[a, b]
            if np.isfinite(value):
                out[(a, b)] = float(value)
    return out


def return_correlations(
    closes: Mapping[str, pd.Series], lookback: int, min_bars: int
) -> dict[tuple[str, str], float]:
    """Correlation of log returns over the last *lookback* bars of the joint timeline.

    *closes* must already be cut at the decision time (causal).  Pairs with fewer than
    *min_bars* overlapping returns are omitted, so :func:`correlation` treats them as 1.
    The research simulator applies :func:`correlations_from_returns` to the same window.
    """
    if not closes:
        return {}
    rets = log_returns(closes)
    return correlations_from_returns(rets.iloc[-lookback:], min_bars)


def with_marks(state: PortfolioState, marks: Mapping[str, float]) -> PortfolioState:
    return replace(
        state,
        positions=tuple(replace(p, mark=marks.get(p.symbol, p.mark)) for p in state.positions),
    )
