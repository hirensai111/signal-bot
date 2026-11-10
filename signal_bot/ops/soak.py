"""Soak-test report: did the paper deployment run correctly over the last N days?

    docker compose exec bot python -m signal_bot.ops.soak --prometheus http://prometheus:9090 --days 14
    # from the host (ports are bound to localhost):
    python -m signal_bot.ops.soak --prometheus http://127.0.0.1:9090 --days 7 --markdown soak.md
    # the second paper bot (compose profile "tp"):
    docker compose --profile tp exec bot-tp python -m signal_bot.ops.soak \
        --prometheus http://prometheus:9090 --instance bot-tp:8080

It reads Prometheus (and, with --database-url or DATABASE_URL, the trade tables) and
checks the operational pass criteria in docs/deployment.md.  It also prints the
resource figures to size the server with: peak bot memory, disk use and growth,
database and backup size.

Exit code 0 = every criterion passed.  The criteria are about operations (did it run,
decide on every 4h close, keep stops in place, reconcile, back up, alert), not about
whether the strategy made money.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from dataclasses import dataclass
from typing import Any

import aiohttp

BARS_PER_DAY = 6  # 4h


@dataclass
class Criterion:
    name: str
    passed: bool
    detail: str


_BOT_SERIES = re.compile(r"\b(up|(?:signalbot|process)_[a-z0-9_]+)(\{)?")


def for_instance(expr: str, instance: str | None) -> str:
    """Restrict every bot series in *expr* to one bot (two paper bots may run side by side).

    Backup metrics come from node-exporter, not a bot, and are left alone.
    """
    if not instance:
        return expr
    sel = f'instance="{instance}"'

    def add(m: re.Match) -> str:
        name, brace = m.group(1), m.group(2)
        if name.startswith("signalbot_backup_"):
            return m.group(0)
        return f"{name}{{{sel}, " if brace else f"{name}{{{sel}}}"

    return _BOT_SERIES.sub(add, expr)


class Prom:
    def __init__(
        self, base: str, session: aiohttp.ClientSession, instance: str | None = None
    ) -> None:
        self.base = base.rstrip("/")
        self.session = session
        self.instance = instance

    async def query(self, expr: str) -> list[dict[str, Any]]:
        expr = for_instance(expr, self.instance)
        async with self.session.get(f"{self.base}/api/v1/query", params={"query": expr}) as resp:
            body = await resp.json()
        if body.get("status") != "success":
            raise RuntimeError(f"query failed: {expr}: {body.get('error')}")
        return body["data"]["result"]

    async def scalar(self, expr: str) -> float | None:
        rows = await self.query(expr)
        return float(rows[0]["value"][1]) if rows else None

    async def by(self, expr: str, label: str) -> dict[str, float]:
        return {r["metric"].get(label, ""): float(r["value"][1]) for r in await self.query(expr)}


def _gb(x: float | None) -> str:
    return "n/a" if x is None else f"{x / 1e9:.2f} GB"


async def collect(prom: Prom, days: int) -> tuple[list[Criterion], dict[str, Any]]:
    w = f"{days}d"
    bot = 'job="signal-bot"'
    facts: dict[str, Any] = {}
    crit: list[Criterion] = []

    uptime = await prom.scalar(f"avg_over_time(up{{{bot}}}[{w}])")
    facts["uptime_pct"] = None if uptime is None else round(uptime * 100, 3)
    crit.append(
        Criterion(
            "Bot reachable >= 99% of the time",
            uptime is not None and uptime >= 0.99,
            f"{facts['uptime_pct']}% of scrapes",
        )
    )

    restarts = await prom.scalar(f"changes(process_start_time_seconds{{{bot}}}[{w}])") or 0.0
    facts["restarts"] = int(restarts)
    crit.append(
        Criterion(
            "At most one unplanned restart a week",
            restarts <= max(1, days // 7),
            f"{int(restarts)} restart(s) (deploys count too: note them)",
        )
    )

    tradable = await prom.by("signalbot_trend_symbol_tradable == 1", "symbol")
    bars = await prom.by(
        f'sum by (symbol) (increase(signalbot_trend_candles_total{{outcome="appended"}}[{w}]))',
        "symbol",
    )
    expected = days * BARS_PER_DAY
    short = {s: round(bars.get(s, 0.0)) for s in tradable if bars.get(s, 0.0) < 0.95 * expected}
    facts["bars_expected_per_symbol"] = expected
    facts["bars_by_symbol"] = {s: round(v) for s, v in sorted(bars.items())}
    crit.append(
        Criterion(
            "Every tradable symbol processed >= 95% of its 4h closes",
            bool(tradable) and not short,
            f"expected {expected} per symbol; short: {short}"
            if short or not tradable
            else f"{len(tradable)} symbols, all >= {int(0.95 * expected)} of {expected}",
        )
    )

    batches = await prom.scalar(f"increase(signalbot_trend_batches_total[{w}])") or 0.0
    facts["decision_batches"] = round(batches)

    unprotected = await prom.scalar(f"max_over_time(signalbot_positions_unprotected[{w}])") or 0.0
    protective = (
        await prom.scalar(f"increase(signalbot_protective_order_failures_total[{w}])") or 0.0
    )
    crit.append(
        Criterion(
            "No position ever without a stop",
            unprotected == 0 and protective == 0,
            f"max unprotected {int(unprotected)}, protective failures {round(protective)}",
        )
    )

    trail_bad = (
        await prom.scalar(
            f'sum(increase(signalbot_trailing_stop_updates_total{{outcome=~"rejected|unknown"}}[{w}]))'
        )
        or 0.0
    )
    trail_moved = (
        await prom.scalar(
            f'sum(increase(signalbot_trailing_stop_updates_total{{outcome="moved"}}[{w}]))'
        )
        or 0.0
    )
    facts["trailing_moves"] = round(trail_moved)
    crit.append(
        Criterion(
            "Trailing-stop amendments succeed",
            trail_bad == 0,
            f"{round(trail_moved)} moved, {round(trail_bad)} refused/unknown",
        )
    )

    recon_fail = await prom.scalar(f"increase(signalbot_reconciliation_failures_total[{w}])") or 0.0
    recon_age = await prom.scalar(
        "time() - signalbot_reconciliation_last_success_timestamp_seconds"
    )
    crit.append(
        Criterion(
            "Reconciliation keeps succeeding",
            recon_age is not None and recon_age < 900,
            f"{round(recon_fail)} failed passes; last success {recon_age and round(recon_age)}s ago",
        )
    )

    backup_age = await prom.scalar("time() - signalbot_backup_last_success_timestamp_seconds")
    crit.append(
        Criterion(
            "Daily backups",
            backup_age is not None and backup_age < 36 * 3600,
            f"last backup {backup_age and round(backup_age / 3600, 1)}h ago",
        )
    )

    alerts = await prom.by(
        f'max by (alertname) (max_over_time(ALERTS{{alertstate="firing", alertname!="Watchdog"}}[{w}]))',
        "alertname",
    )
    facts["alerts_fired"] = sorted(alerts)
    watchdog = await prom.scalar('ALERTS{alertname="Watchdog", alertstate="firing"}')
    crit.append(
        Criterion(
            "Alert pipeline alive (Watchdog firing)",
            watchdog == 1.0,
            "fired during the window: " + (", ".join(sorted(alerts)) or "none"),
        )
    )

    facts["bot_memory_peak"] = _gb(
        await prom.scalar(f"max_over_time(process_resident_memory_bytes{{{bot}}}[{w}])")
    )
    facts["bot_memory_now"] = _gb(await prom.scalar(f"process_resident_memory_bytes{{{bot}}}"))
    free_now = await prom.scalar('node_filesystem_avail_bytes{mountpoint="/"}')
    free_min = await prom.scalar(
        f'min_over_time(node_filesystem_avail_bytes{{mountpoint="/"}}[{w}])'
    )
    free_then = await prom.scalar(f'node_filesystem_avail_bytes{{mountpoint="/"}} offset {w}')
    size = await prom.scalar('node_filesystem_size_bytes{mountpoint="/"}')
    facts["disk_free_now"] = _gb(free_now)
    facts["disk_free_min"] = _gb(free_min)
    if free_now is not None and free_then is not None:
        per_day = (free_then - free_now) / days
        facts["disk_used_per_day"] = _gb(per_day)
        facts["days_until_disk_full"] = round(free_now / per_day) if per_day > 0 else None
    facts["disk_size"] = _gb(size)
    facts["memory_available_min"] = _gb(
        await prom.scalar(f"min_over_time(node_memory_MemAvailable_bytes[{w}])")
    )
    facts["backup_size"] = _gb(await prom.scalar("signalbot_backup_last_size_bytes"))
    if free_now is not None and size:
        crit.append(
            Criterion(
                "Disk has >= 20% free",
                free_now / size >= 0.2,
                f"{_gb(free_now)} free of {_gb(size)}",
            )
        )
    return crit, facts


async def trading_facts(database_url: str, days: int) -> dict[str, Any]:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT count(*), coalesce(sum(net_pnl), 0), avg(realized_r), "
                        "coalesce(sum(fees), 0) FROM trade_results "
                        "WHERE closed_at >= now() - make_interval(days => :d)"
                    ),
                    {"d": days},
                )
            ).one()
            sig = (
                await conn.execute(
                    text(
                        "SELECT count(*) FILTER (WHERE status <> 'cancelled'), "
                        "count(*) FILTER (WHERE cancelled_reason LIKE 'portfolio:%') FROM signals "
                        "WHERE strategy = 'trend_breakout' AND created_at >= now() - make_interval(days => :d)"
                    ),
                    {"d": days},
                )
            ).one()
            db_size = (
                await conn.execute(text("SELECT pg_database_size(current_database())"))
            ).scalar_one()
    finally:
        await engine.dispose()
    return {
        "closed_trades": row[0],
        "net_pnl": round(float(row[1]), 2),
        "avg_r": None if row[2] is None else round(float(row[2]), 4),
        "fees": round(float(row[3]), 2),
        "signals_taken": sig[0],
        "signals_refused_by_portfolio": sig[1],
        "database_size": _gb(float(db_size)),
    }


def render(crit: list[Criterion], facts: dict[str, Any], days: int) -> str:
    lines = [
        f"# Soak report: last {days} days",
        "",
        "| Criterion | Result | Detail |",
        "|---|---|---|",
    ]
    lines += [f"| {c.name} | {'PASS' if c.passed else 'FAIL'} | {c.detail} |" for c in crit]
    lines += ["", "## Facts", "", "| | |", "|---|---|"]
    lines += [f"| {k.replace('_', ' ')} | {v} |" for k, v in facts.items()]
    verdict = "PASSED" if all(c.passed for c in crit) else "NOT PASSED"
    lines += ["", f"**{verdict}** ({sum(c.passed for c in crit)}/{len(crit)} criteria)"]
    return "\n".join(lines)


async def run(
    prometheus: str, days: int, database_url: str | None, instance: str | None = "bot:8080"
) -> tuple[str, bool]:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        crit, facts = await collect(Prom(prometheus, session, instance), days)
    facts = {"bot": instance or "all", **facts}
    if database_url:
        try:
            facts.update(await trading_facts(database_url, days))
        except Exception as exc:  # the report is still useful without the database
            facts["trading_facts_error"] = f"{type(exc).__name__}: {exc}"
    return render(crit, facts, days), all(c.passed for c in crit)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--prometheus", default="http://127.0.0.1:9090")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument(
        "--instance",
        default="bot:8080",
        help="which paper bot (bot:8080 = main, bot-tp:8080 = second bot; '' = no filter)",
    )
    ap.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    ap.add_argument("--markdown", help="also write the report to this file")
    args = ap.parse_args()
    report, ok = asyncio.run(
        run(args.prometheus, args.days, args.database_url, args.instance or None)
    )
    print(report)
    if args.markdown:
        with open(args.markdown, "w") as fh:
            fh.write(report + "\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
