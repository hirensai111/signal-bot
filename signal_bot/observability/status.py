"""Aggregates live system state for the control room and readiness checks.

Every section degrades independently: if the database is down, risk and order
sections report the error while positions (in-memory, from the venue stream)
and component health keep working.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from signal_bot.db.models import OperatorAudit, Order

logger = logging.getLogger(__name__)


def _iso(value):
    return value.isoformat() if isinstance(value, datetime) else value


@dataclass
class StatusService:
    environment: dict[str, Any]
    governor: Any
    bus: Any
    order_store: Any
    session_factory: Any
    reconciler: Any = None
    adapter: Any = None
    position_view: Any = None
    fanout: Any = None
    writer: Any = None
    engine: Any = None
    stream: Any = None  # object with .connected
    ingestor: Any = None  # object with .last_error
    replay_counts: dict[str, int] = field(default_factory=dict)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    max_candle_lag_seconds: float = 90.0

    # ------------------------------------------------------------------
    def market_data(self) -> dict[str, Any]:
        if self.fanout is None:
            return {}
        out: dict[str, dict[str, float]] = {}
        for (symbol, interval), age in self.fanout.ages(datetime.now(timezone.utc)).items():
            out.setdefault(symbol, {})[interval] = round(age, 1)
        return out

    def components(self) -> list[dict[str, Any]]:
        comps = []
        md = self.market_data()
        if self.fanout is not None:
            lags = [ages.get("1m") for ages in md.values() if ages.get("1m") is not None]
            worst = max(lags) if lags else None
            ok = worst is not None and worst <= self.max_candle_lag_seconds
            error = getattr(self.ingestor, "last_error", None)
            if error:
                state, detail = "degraded", f"cannot connect: {error}"
            elif worst is None:
                state, detail = "starting", "no candles yet"
            else:
                state = "healthy" if ok else "degraded"
                detail = f"newest 1m candle closed {worst:.0f}s ago"
            comps.append({"name": "Market data stream", "status": state, "detail": detail})
        if self.stream is not None:
            connected = bool(getattr(self.stream, "connected", False))
            comps.append(
                {
                    "name": "Exchange private stream",
                    "status": "healthy" if connected else "degraded",
                    "detail": "connected" if connected else "disconnected",
                }
            )
        if self.adapter is not None and hasattr(self.adapter, "account_summary"):
            err = self.adapter.account_summary().get("book_source_error")
            comps.append(
                {
                    "name": "Paper broker",
                    "status": "degraded" if err else "healthy",
                    "detail": err or "matching on live order books",
                }
            )
        if self.governor is not None:
            gate_open, why = self.governor.gate_open()
            comps.append(
                {
                    "name": "Risk governor",
                    "status": "healthy" if self.governor.healthy else "failed",
                    "detail": "enforcing; gate open" if gate_open else f"enforcing; blocked: {why}",
                }
            )
        if self.reconciler is not None:
            comps.append(
                {
                    "name": "Order reconciliation",
                    "status": "healthy" if self.reconciler.last_ok else "degraded",
                    "detail": (
                        f"last run {_iso(self.reconciler.last_run_at)}; "
                        f"{len(self.reconciler.last_discrepancies)} discrepancies"
                    )
                    if self.reconciler.last_run_at
                    else "not run yet",
                }
            )
        if self.bus is not None:
            stats = self.bus.stats()
            backlog = sum(s["lag"] for s in stats["subscribers"].values())
            comps.append(
                {
                    "name": "Event journal",
                    "status": "healthy" if stats["journal_healthy"] else "degraded",
                    "detail": f"backlog {backlog}; {stats['journal_pending']} buffered writes",
                }
            )
        if self.writer is not None:
            comps.append(
                {
                    "name": "Candle storage",
                    "status": "healthy" if self.writer.healthy else "degraded",
                    "detail": "persisting" if self.writer.healthy else "flush failing; retrying",
                }
            )
        return comps

    async def risk(self) -> dict[str, Any]:
        if self.governor is None:
            return {"error": "no governor"}
        try:
            status = await self.governor.get_status()
            return {k: _iso(v) if not isinstance(v, dict) else v for k, v in status.items()}
        except Exception as exc:
            return {**self.governor.cached_status(), "error": f"risk state unavailable: {exc}"}

    async def orders(self, limit: int = 25) -> list[dict[str, Any]] | dict:
        try:
            rows: list[Order] = await self.order_store.recent(limit)
        except Exception as exc:
            return {"error": str(exc)}
        return [
            {
                "client_order_id": o.client_order_id,
                "symbol": o.symbol,
                "side": o.side,
                "state": o.state,
                "size": str(o.size),
                "filled": str(o.filled_size),
                "avg_price": None if o.avg_fill_price is None else str(o.avg_fill_price),
                "protective": o.protective_status,
                "reason": o.state_reason,
                "updated_at": _iso(o.updated_at),
            }
            for o in rows
        ]

    async def audit(self, limit: int = 20) -> list[dict[str, Any]] | dict:
        try:
            async with self.session_factory() as session:
                rows = await session.execute(
                    select(OperatorAudit).order_by(OperatorAudit.id.desc()).limit(limit)
                )
                return [
                    {
                        "at": _iso(r.created_at),
                        "actor": r.actor,
                        "action": r.action,
                        "reason": r.reason,
                        "succeeded": r.succeeded,
                    }
                    for r in rows.scalars()
                ]
        except Exception as exc:
            return {"error": str(exc)}

    async def snapshot(self) -> dict[str, Any]:
        comps = self.components()
        risk = await self.risk()
        halted = (
            bool(risk.get("kill_switch_active"))
            or bool(risk.get("circuit_breakers"))
            or bool(risk.get("blocking_reason"))
        )
        if any(c["status"] == "failed" for c in comps):
            overall = "failed"
        elif halted:
            overall = "halted"
        elif any(c["status"] in ("degraded", "starting") for c in comps):
            overall = "degraded"
        else:
            overall = "healthy"
        account = None
        curve: list = []
        if self.adapter is not None and hasattr(self.adapter, "account_summary"):
            account = self.adapter.account_summary()
            curve = list(getattr(self.adapter, "equity_curve", []))[-500:]
        bus_stats = self.bus.stats() if self.bus is not None else {}
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": int((datetime.now(timezone.utc) - self.started_at).total_seconds()),
            "environment": self.environment,
            "overall": overall,
            "components": comps,
            "risk": risk,
            "account": account,
            "equity_curve": curve,
            "positions": self.position_view.snapshot() if self.position_view else [],
            "orders": await self.orders(),
            "discrepancies": list(getattr(self.reconciler, "last_discrepancies", []) or []),
            "event_bus": bus_stats,
            "event_backlog": sum(s["lag"] for s in bus_stats.get("subscribers", {}).values()),
            "replay": self.replay_counts,
            "market_data_age_seconds": self.market_data(),
            "audit": await self.audit(),
        }

    def readiness(self) -> tuple[bool, list[str]]:
        reasons = []
        if self.governor is not None:
            ok, why = self.governor.gate_open()
            if not ok:
                reasons.append(why)
        for comp in self.components():
            if comp["status"] in ("failed",):
                reasons.append(f"{comp['name']}: {comp['detail']}")
        return (not reasons), reasons
