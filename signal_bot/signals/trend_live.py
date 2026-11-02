"""Live 4h trend-breakout signals, decided on completed candles with the research code.

Pipeline (one task, so the order of operations at a bar close is fixed):

    closed 4h candle ─► CandleHistory.append ─► TrailingStopManager.on_bar_close
                     └► TrendSignalGenerator (signals.trend.trend_decisions on the history)
                     └► batch by bar close ─► rank ─► portfolio rules ─► Signal rows ─► engine

* **Completed candles only.** Candles that are still forming, duplicated or out
  of order never reach the decision (see :class:`CandleHistory`).  Candles that
  are complete but late (REST backfill, a reconnect) update history but never
  trade: research decides at the close and executes at the next open.
* **Same decisions as research.** The generator calls the research decision
  function on the history it holds and keeps the newest bar's decision.  A
  symbol trades only once it holds ``min_history_bars`` completed bars (the
  EMA200 starts from the first stored bar, so it needs a long warm-up; see
  docs/research.md, "Live parity").
* **Simultaneous entries.** All decisions for one bar close are collected until
  every tradable symbol's candle for that bar has arrived (or the batch times
  out), then ranked and admitted together by the portfolio rules, so arrival
  order on the socket cannot change which trades are taken.
* **Research parity rule.** A symbol that held a position at any time during the
  decision bar is skipped, as in the fill model (one position per symbol, and a
  signal on the exit bar is not taken).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import aiohttp

from signal_bot.core.clock import SYSTEM_CLOCK, Clock
from signal_bot.db.models import RiskState, Signal
from signal_bot.events.envelope import Event, EventType
from signal_bot.execution.orders import EXPOSURE_STATES
from signal_bot.features.candle_history import CandleHistory
from signal_bot.ingest.binance_ws import DataIngestor, Kline
from signal_bot.observability import metrics
from signal_bot.observability.logging import bind
from signal_bot.risk.portfolio import (
    Admission,
    Candidate,
    ContractSpec,
    OpenPosition,
    PortfolioLimits,
    PortfolioState,
    admit,
    exposure_summary,
    return_correlations,
)
from signal_bot.signals.trend import TrendDecision, TrendParams, trend_decisions

logger = logging.getLogger(__name__)

STRATEGY = "trend_breakout"
BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/klines"

KlineFetcher = Callable[[str, str, int], Awaitable[list[Kline]]]


class TrendSignalGenerator:
    """Evaluates the research decision rule on the newest completed bar."""

    def __init__(
        self, params: TrendParams, history: CandleHistory, min_history_bars: int = 1000
    ) -> None:
        if history.interval != params.timeframe:
            raise ValueError("history interval must match the strategy timeframe")
        self.params = params
        self.history = history
        self.min_history_bars = max(min_history_bars, params.warmup_bars)

    def ready(self, symbol: str) -> bool:
        return self.history.bars(symbol) >= self.min_history_bars

    def evaluate_last(self, symbol: str) -> TrendDecision | None:
        if not self.ready(symbol):
            return None
        frame = self.history.frame(symbol)
        last = len(frame) - 1
        found = trend_decisions(frame, self.params, start=last)
        return found[-1] if found and found[-1].bar == last else None

    def on_candle(self, candle: Kline) -> tuple[str, TrendDecision | None]:
        outcome = self.history.append(candle)
        if outcome != "appended":
            return outcome, None
        return outcome, self.evaluate_last(candle.symbol)


async def binance_rest_klines(symbol: str, interval: str, limit: int = 1500) -> list[Kline]:
    """Completed USD-M futures klines from Binance REST (public; the max limit is 1500)."""
    params = {"symbol": symbol, "interval": interval, "limit": str(min(limit, 1500))}
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(BINANCE_KLINES, params=params) as resp:
            resp.raise_for_status()
            rows = await resp.json()
    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    # The last REST kline is usually still forming: only rows whose close time has passed.
    return [
        DataIngestor._rest_kline_to_dataclass(symbol, interval, r) for r in rows if r[6] < now_ms
    ]


class LivePortfolio:
    """Builds the portfolio rules' inputs from the venue, the order store and history."""

    def __init__(
        self,
        adapter,
        store,
        history: CandleHistory,
        session_factory,
        limits: PortfolioLimits,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        self.adapter = adapter
        self.store = store
        self.history = history
        self.session_factory = session_factory
        self.limits = limits
        self.clock = clock
        self.last_summary: dict[str, Any] | None = None

    def spec(self, symbol: str) -> ContractSpec | None:
        try:
            inst = self.adapter.instrument(symbol)
        except (KeyError, RuntimeError):
            return None
        return ContractSpec(
            float(inst.contract_value),
            float(inst.max_contracts) if inst.max_contracts is not None else None,
        )

    async def equity_and_peak(self) -> tuple[float, float]:
        """Current equity, and the persisted peak (raised to equity when exceeded)."""
        equity = Decimal(str(await self.adapter.account_equity()))
        async with self.session_factory() as session:
            state = await session.get(RiskState, 1)
            if state is None:
                state = RiskState(
                    id=1,
                    kill_switch_active=False,
                    daily_pnl_r=Decimal("0"),
                    daily_trade_count=0,
                    consecutive_losses=0,
                    current_open_positions=0,
                )
                session.add(state)
            if state.peak_equity is None or equity > state.peak_equity:
                state.peak_equity = equity
                state.peak_equity_at = self.clock.now()
            peak = state.peak_equity
            await session.commit()
        return float(equity), float(peak)

    async def positions(self) -> list[OpenPosition]:
        out = []
        for order in await self.store.in_states(EXPOSURE_STATES):
            symbol = self.adapter.binance_symbol(order.symbol) or order.symbol
            cv = float(order.contract_value or 1)
            contracts = float(order.filled_size or 0) or float(order.size or 0)
            basis = float(order.price_basis or 0)
            entry = float(order.avg_fill_price or order.intended_entry) - basis
            stop = float(order.current_stop_price or order.stop_loss_price) - basis
            mark = self.history.last_close_price(symbol) or entry
            out.append(
                OpenPosition(
                    symbol=symbol,
                    side="long" if order.side == "buy" else "short",
                    qty=contracts * cv,
                    entry=entry,
                    mark=mark,
                    stop=stop,
                )
            )
        return out

    async def state(self, at: datetime | None = None) -> PortfolioState:
        equity, peak = await self.equity_and_peak()
        positions = await self.positions()
        corr = return_correlations(
            self.history.closes(until=at),
            self.limits.correlation_lookback_bars,
            self.limits.correlation_min_bars,
        )
        state = PortfolioState(equity, peak, tuple(positions), corr)
        summary = exposure_summary(state, self.limits)
        self.last_summary = {**summary, "as_of": self.clock.now().isoformat()}
        metrics.PORTFOLIO_OPEN_RISK.set(summary["open_risk_pct"] / 100)
        metrics.PORTFOLIO_CORRELATED_RISK.set(summary["correlated_risk_pct"] / 100)
        metrics.PORTFOLIO_DRAWDOWN.set(summary["drawdown_pct"] / 100)
        return state


class TrendPipeline:
    def __init__(
        self,
        generator: TrendSignalGenerator,
        portfolio: LivePortfolio,
        candle_queue: asyncio.Queue,
        signal_queue: asyncio.Queue,
        bus,
        session_factory,
        *,
        universe: list[str],
        tradable: set[str],
        trailing=None,
        telegram=None,
        strategy: dict | None = None,
        clock: Clock = SYSTEM_CLOCK,
        batch_timeout: float = 120.0,
        max_candle_lag: float = 90.0,
    ) -> None:
        self.generator = generator
        self.portfolio = portfolio
        self.limits = portfolio.limits
        self.history = generator.history
        self.candle_queue = candle_queue
        self.signal_queue = signal_queue
        self.bus = bus
        self.session_factory = session_factory
        self.universe = sorted(universe)
        self.tradable = set(tradable)
        self.trailing = trailing
        self.telegram = telegram
        self.strategy = strategy or {"name": STRATEGY}
        self.clock = clock
        self.batch_timeout = batch_timeout
        self.max_candle_lag = max_candle_lag
        self.step = self.history.step
        self._batches: dict[datetime, dict[str, TrendDecision | None]] = {}
        self._shutdown = asyncio.Event()
        self.last_batch: dict[str, Any] | None = None
        self.counts: dict[str, int] = {}
        self.warmup_errors: dict[str, str] = {}

    # ------------------------------------------------------------------
    async def warmup(self, fetch: KlineFetcher, limit: int = 1500) -> None:
        for symbol in self.universe:
            try:
                klines = await fetch(symbol, self.history.interval, limit)
            except Exception as exc:  # e.g. HTTP 451 from Binance in some regions
                self.warmup_errors[symbol] = f"{type(exc).__name__}: {exc}"
                logger.error("Trend warm-up for %s failed: %s", symbol, exc)
                continue
            n = self.history.seed(klines)
            logger.info(
                "Trend warm-up %s: %d completed %s bars (ready=%s)",
                symbol,
                n,
                self.history.interval,
                self.generator.ready(symbol),
            )

    def shutdown(self) -> None:
        self._shutdown.set()

    async def run(self) -> None:
        logger.info("Trend pipeline started (universe=%s)", self.universe)
        while not self._shutdown.is_set():
            try:
                candle = await asyncio.wait_for(self.candle_queue.get(), timeout=1.0)
            except TimeoutError:
                candle = None
            try:
                if candle is not None:
                    await self.handle_candle(candle)
                await self.flush_due()
            except Exception:
                logger.exception("Trend pipeline iteration failed")

    # ------------------------------------------------------------------
    async def handle_candle(self, candle: Kline) -> None:
        if candle.interval != self.history.interval or candle.symbol not in self.universe:
            return
        outcome, decision = self.generator.on_candle(candle)
        self.counts[outcome] = self.counts.get(outcome, 0) + 1
        if outcome != "appended":
            return
        if self.trailing is not None:
            try:
                await self.trailing.on_bar_close(candle)
            except Exception:
                logger.exception("Trailing update for %s failed", candle.symbol)
        close = candle.open_time + self.step
        lag = (self.clock.now() - close).total_seconds()
        if lag > self.max_candle_lag:
            return  # history only: a late bar must never trade
        batch = self._batches.setdefault(close, {})
        batch[candle.symbol] = decision
        if self._batch_complete(close):
            await self.flush(close)

    def _batch_complete(self, close: datetime) -> bool:
        batch = self._batches.get(close, {})
        expected = {s for s in self.universe if s in self.tradable and self.generator.ready(s)}
        return expected.issubset(batch)

    async def flush_due(self) -> None:
        now = self.clock.now()
        for close in sorted(self._batches):
            if now >= close + timedelta(seconds=self.batch_timeout):
                await self.flush(close)

    async def flush(self, close: datetime) -> list[Admission]:
        batch = self._batches.pop(close, {})
        decisions = {s: d for s, d in batch.items() if d is not None}
        admissions = await self.decide(close, decisions) if decisions else []
        self.last_batch = {
            "bar_close": close.isoformat(),
            "symbols_reported": sorted(batch),
            "decisions": [
                {
                    "symbol": a.candidate.symbol,
                    "side": a.candidate.side,
                    "strength": round(a.candidate.strength, 4),
                    "accepted": a.accepted,
                    "reason": a.reason,
                    "contracts": a.contracts,
                    "risk_pct": round(a.risk_fraction * 100, 4),
                }
                for a in admissions
            ],
        }
        return admissions

    async def decide(self, close: datetime, decisions: dict[str, TrendDecision]) -> list[Admission]:
        bar_open = close - self.step
        busy_venue = await self.portfolio.store.symbols_active_since(bar_open, EXPOSURE_STATES)
        busy = {self.portfolio.adapter.binance_symbol(s) or s for s in busy_venue}
        refused: list[tuple[str, TrendDecision, str]] = []
        candidates: list[Candidate] = []
        for symbol, d in sorted(decisions.items()):
            spec = self.portfolio.spec(symbol) if symbol in self.tradable else None
            if spec is None:
                refused.append((symbol, d, "symbol_unmapped"))
            elif symbol in busy:
                refused.append((symbol, d, "symbol_busy_this_bar"))
            else:
                candidates.append(Candidate(symbol, d.side, d.entry, d.stop, d.strength, spec, d))
        try:
            state = await self.portfolio.state(at=close)
        except Exception as exc:
            logger.exception("Portfolio state unavailable; refusing the batch")
            state = None
            reason = f"portfolio_state_unavailable: {type(exc).__name__}"
            admissions = [Admission(c, False, reason) for c in candidates]
        if state is not None:
            admissions = admit(state, candidates, self.limits)
        admissions += [
            Admission(Candidate(symbol, d.side, d.entry, d.stop, d.strength, ref=d), False, why)
            for symbol, d, why in refused
        ]
        accepted = [a for a in admissions if a.accepted]
        for rank, a in enumerate(admissions, start=1):
            await self._record(a, rank, len(admissions), close, state)
        logger.info(
            "Trend batch %s: %d decisions, %d accepted",
            close.isoformat(),
            len(admissions),
            len(accepted),
        )
        return admissions

    async def _record(self, a: Admission, rank: int, size: int, close: datetime, state) -> None:
        d: TrendDecision = a.candidate.ref
        outcome = "accepted" if a.accepted else (a.reason or "refused").split(":")[0]
        metrics.TREND_DECISIONS.labels(outcome).inc()
        p = self.generator.params
        equity = state.equity if state is not None else 0.0
        leverage = self.limits.position_leverage
        features = {
            "strategy": STRATEGY,
            "bar_open": (close - self.step).isoformat(),
            "bar_close": close.isoformat(),
            "atr": d.atr,
            "channel": d.channel,
            "ema": d.ema,
            "volume_ratio": d.volume_ratio,
            "strength": d.strength,
            "rank": rank,
            "batch_size": size,
            "contracts": a.contracts,
            "portfolio": None
            if state is None
            else {k: v for k, v in exposure_summary(state, self.limits).items() if k != "limits"},
        }
        sig = Signal(
            id=uuid.uuid4(),
            symbol=a.candidate.symbol,
            side=d.side,
            entry=Decimal(str(d.entry)),
            stop_loss=Decimal(str(d.stop)),
            take_profit=None,
            leverage=leverage,
            risk_pct=Decimal(str(round(a.risk_fraction * 100, 6))),
            account_size_assumed=Decimal(str(round(equity, 8))),
            est_hold_minutes=int(p.max_hold_bars * self.step.total_seconds() // 60),
            pattern_features=features,
            created_at=self.clock.now(),
            status="pending" if a.accepted else "cancelled",
            cancelled_reason=None if a.accepted else f"portfolio:{a.reason}"[:50],
            strategy=STRATEGY,
            trail_distance=Decimal(str(d.trail_distance)),
            trail_from=close,
            time_stop_at=close + self.step * (p.max_hold_bars + 1),
            planned_contracts=Decimal(str(a.contracts)) if a.accepted else None,
        )
        async with self.session_factory() as session:
            session.add(sig)
            await session.commit()
        with bind(correlation_id=str(sig.id), signal_id=str(sig.id)):
            payload = {
                "signal_id": str(sig.id),
                "symbol": sig.symbol,
                "side": sig.side,
                "entry": sig.entry,
                "stop": sig.stop_loss,
                "take_profit": None,
                "trail_distance": sig.trail_distance,
                "time_stop_at": sig.time_stop_at,
                "contracts": a.contracts,
                "rank": rank,
                "batch_size": size,
                "bar_close": close,
                "strategy": self.strategy,
            }
            if a.accepted:
                await self.bus.publish(
                    Event.create(
                        EventType.SIGNAL_GENERATED,
                        payload,
                        "trend_pipeline",
                        correlation_id=str(sig.id),
                    )
                )
                await self.signal_queue.put(sig)
                logger.info(
                    "Trend signal %s %s rank %d/%d contracts=%s",
                    sig.symbol,
                    sig.side,
                    rank,
                    size,
                    a.contracts,
                )
                if self.telegram is not None:
                    try:
                        await self.telegram.notify_signal(sig)
                    except Exception:
                        logger.exception("Telegram signal notification failed")
            else:
                await self.bus.publish(
                    Event.create(
                        EventType.RISK_DENIED,
                        {**payload, "reason": f"portfolio:{a.reason}"},
                        "trend_pipeline",
                        correlation_id=str(sig.id),
                    )
                )
                logger.info("Trend signal %s %s refused: %s", sig.symbol, sig.side, a.reason)

    # ------------------------------------------------------------------
    async def recheck(self, signal: Signal) -> tuple[bool, str | None]:
        """Governor hook: re-apply the portfolio rules just before submission."""
        spec = self.portfolio.spec(signal.symbol)
        if spec is None:
            return False, "portfolio:symbol_unmapped"
        state = await self.portfolio.state()
        cand = Candidate(
            signal.symbol,
            signal.side,
            float(signal.entry),
            float(signal.stop_loss),
            spec=spec,
            fixed_contracts=float(signal.planned_contracts) if signal.planned_contracts else None,
        )
        [verdict] = admit(state, [cand], self.limits)
        return verdict.accepted, None if verdict.accepted else f"portfolio:{verdict.reason}"

    def status(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy,
            "params": self.generator.params.to_dict(),
            "min_history_bars": self.generator.min_history_bars,
            "universe": [
                {
                    "symbol": s,
                    "tradable": s in self.tradable,
                    "bars": self.history.bars(s),
                    "ready": self.generator.ready(s),
                    "last_bar": (lambda t: t.isoformat() if t else None)(self.history.last_open(s)),
                    "warmup_error": self.warmup_errors.get(s),
                }
                for s in self.universe
            ],
            "pending_batches": [c.isoformat() for c in sorted(self._batches)],
            "last_batch": self.last_batch,
            "candles": dict(self.counts),
            "portfolio": self.portfolio.last_summary,
        }


__all__ = ["LivePortfolio", "TrendPipeline", "TrendSignalGenerator", "binance_rest_klines"]
