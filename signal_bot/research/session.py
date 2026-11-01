"""Reports for paper/live trading sessions, built from the order store and event journal.

Covers the categories a backtest cannot: reliability (outages, recovery times,
reconciliation discrepancies, unknown submissions, protective failures, halts)
alongside realised execution quality.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from signal_bot.core.clock import utc
from signal_bot.db.models import EventRecord, OrderTransition, TradeResult
from signal_bot.provenance import provenance


async def session_report(
    session_factory, since: datetime, until: datetime | None = None
) -> dict[str, Any]:
    until = until or datetime.now(timezone.utc)
    async with session_factory() as s:
        trades = list(
            (
                await s.execute(
                    select(TradeResult).where(
                        TradeResult.closed_at >= since, TradeResult.closed_at < until
                    )
                )
            ).scalars()
        )
        events = list(
            (
                await s.execute(
                    select(EventRecord)
                    .where(EventRecord.recorded_at >= since, EventRecord.recorded_at < until)
                    .order_by(EventRecord.sequence)
                )
            ).scalars()
        )
        transitions = list(
            (
                await s.execute(
                    select(OrderTransition).where(
                        OrderTransition.occurred_at >= since, OrderTransition.occurred_at < until
                    )
                )
            ).scalars()
        )

    n = len(trades)
    net = sum(float(t.net_pnl) for t in trades)
    perf = {
        "trades": n,
        "net_pnl": round(net, 2),
        "win_rate": round(sum(float(t.net_pnl) > 0 for t in trades) / n, 4) if n else None,
        "expectancy_r": round(sum(float(t.realized_r) for t in trades) / n, 4) if n else None,
        "signal_expectancy_r": round(sum(float(t.signal_r) for t in trades) / n, 4) if n else None,
        "exit_reasons": _count(t.exit_reason for t in trades),
    }
    execution = {
        "fees": round(sum(float(t.fees) for t in trades), 2),
        "funding": round(sum(float(t.funding) for t in trades), 2),
        "avg_entry_slippage_bps": round(sum(float(t.entry_slippage_bps) for t in trades) / n, 3)
        if n
        else None,
        "fill_ratio": round(
            sum(float(t.size_filled) for t in trades)
            / sum(float(t.size_requested) for t in trades),
            4,
        )
        if n
        else None,
        "partial_fills": sum(float(t.size_filled) < float(t.size_requested) for t in trades),
    }

    outages, down_at = [], None
    for e in events:
        if e.event_type == "STREAM_DISCONNECTED" and down_at is None:
            down_at = utc(e.occurred_at)
        elif e.event_type == "STREAM_CONNECTED" and down_at is not None:
            outages.append((utc(e.occurred_at) - down_at).total_seconds())
            down_at = None
    discrepancies = _count(
        e.payload.get("kind") for e in events if e.event_type == "RECONCILIATION_DISCREPANCY"
    )
    reliability = {
        "applicable": True,
        "stream_outages": len(outages) + (1 if down_at else 0),
        "recovery_seconds": {
            "max": round(max(outages), 1) if outages else None,
            "mean": round(sum(outages) / len(outages), 1) if outages else None,
        },
        "still_down": down_at is not None,
        "reconciliation_discrepancies": discrepancies,
        "missed_events_recovered": discrepancies.get("missed_order_update", 0)
        + discrepancies.get("missed_close", 0),
        "unknown_submissions": sum(t.to_state == "submission_unknown" for t in transitions),
        "protective_failures": sum(e.event_type == "PROTECTIVE_ORDER_FAILED" for e in events),
        "risk_halts": sum(e.event_type == "RISK_HALT" for e in events),
        "risk_denials": sum(e.event_type == "RISK_DENIED" for e in events),
    }
    report = {
        "schema_version": 1,
        "kind": "session",
        "window": [since.isoformat(), until.isoformat()],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "performance": perf,
        "execution": execution,
        "reliability": reliability,
        "provenance": provenance(),
    }
    report["report_id"] = hashlib.sha256(
        json.dumps(
            {k: report[k] for k in ("window", "performance", "reliability")}, default=str
        ).encode()
    ).hexdigest()[:16]
    return report


def _count(values) -> dict[str, int]:
    out: dict[str, int] = {}
    for v in values:
        if v is None:
            continue
        out[str(v)] = out.get(str(v), 0) + 1
    return out
