"""What-if: replay a paper bot's signal history under another bot's portfolio rules.

Read-only.  Takes every trend candidate this bot's database recorded (accepted and
refused), and decides each bar again with the portfolio rules of ``--strategy``, using
the same admission code as the live bot (``risk/portfolio.py``).  Trades both rule sets
take keep their real paper outcome; trades only the other rules take are simulated on
Delta one-minute trade candles (entry at the open of the first minute after the bar
close, stop first if both levels are touched in one minute, taker fees on both sides,
no funding); trades only this bot took are removed.

Run inside the second bot's container, e.g. on the server:

    docker compose exec -T bot-tp python -m signal_bot.ops.whatif \
        --strategy signal_bot/research/strategies/trend_portfolio_tp15_pause.toml
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import tomllib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import aiohttp
import pandas as pd
from sqlalchemy import select

import signal_bot
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import Order, Signal, TradeResult
from signal_bot.delta.client import ENDPOINTS
from signal_bot.ops.fork_tp import historical_exit
from signal_bot.risk.portfolio import (
    Candidate,
    ContractSpec,
    OpenPosition,
    PortfolioLimits,
    PortfolioState,
    admit,
    return_correlations,
)
from signal_bot.signals.trend_live import STRATEGY

PACKAGE = Path(signal_bot.__file__).resolve().parent  # also when run from stdin
UNIVERSE = PACKAGE / "research" / "delta_universe.json"
STEP = timedelta(hours=4)
TAKER_FEE = 0.0005
BINANCE = "https://fapi.binance.com/fapi/v1/klines"
DELTA = f"{ENDPOINTS['production']['rest']}/v2/history/candles"


def utc(ts: datetime) -> datetime:
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


@dataclass
class Trade:
    symbol: str
    side: str
    qty: float
    entry: float  # Binance-referenced, as the portfolio rules see it
    stop: float
    opened: datetime
    closed: datetime | None
    pnl: float | None  # net, when closed
    kind: str  # "real" | "extra"
    exit_reason: str = ""
    basis: float = 0.0  # Delta minus Binance at entry (extras)
    r: float | None = None

    @property
    def sign(self) -> float:
        return 1.0 if self.side == "long" else -1.0

    def open_at(self, t: datetime) -> bool:
        return self.opened < t and (self.closed is None or self.closed > t)

    def value_at(self, t: datetime, mark: float | None) -> float:
        """Net P&L counted in equity at *t* (realized, or marked to *mark*)."""
        if self.closed is not None and self.closed <= t:
            return self.pnl or 0.0
        if mark is None:
            return 0.0
        return self.sign * (mark - self.entry) * self.qty - TAKER_FEE * self.entry * self.qty


def resolve(path: str) -> Path:
    p = Path(path)
    if p.exists():
        return p
    alt = PACKAGE.parent / path
    if alt.exists():
        return alt
    raise SystemExit(f"strategy config not found: {path}")


async def binance_closes(http, symbol: str, start: datetime) -> pd.Series | None:
    try:
        params = {
            "symbol": symbol,
            "interval": "4h",
            "startTime": int(start.timestamp() * 1000),
            "limit": 1500,
        }
        async with http.get(BINANCE, params=params) as r:
            if r.status != 200:
                return None
            rows = await r.json()
    except aiohttp.ClientError:
        return None
    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    data = {
        pd.Timestamp(int(row[6]) + 1, unit="ms", tz="UTC"): float(row[4])
        for row in rows
        if row[6] < now_ms
    }
    return pd.Series(data).sort_index() if data else None


async def delta_candles(http, symbol: str, resolution: str, start: int, end: int) -> list[dict]:
    span = {"1m": 60, "4h": 14400}[resolution] * 1900
    out: list[dict] = []
    for left in range(start, end, span):
        right = min(end, left + span)
        params = {"resolution": resolution, "symbol": symbol, "start": left, "end": right}
        async with http.get(DELTA, params=params) as r:
            r.raise_for_status()
            payload = await r.json()
        out += [row for row in payload.get("result", []) if left <= int(row["time"]) < right]
    return sorted({int(r["time"]): r for r in out}.values(), key=lambda r: int(r["time"]))


async def delta_closes(http, symbol: str, start: datetime) -> pd.Series | None:
    now = int(datetime.now(timezone.utc).timestamp())
    rows = await delta_candles(http, symbol, "4h", int(start.timestamp()), now)
    data = {
        pd.Timestamp(int(r["time"]) + 14400, unit="s", tz="UTC"): float(r["close"])
        for r in rows
        if int(r["time"]) + 14400 <= now
    }
    return pd.Series(data).sort_index() if data else None


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--strategy", required=True, help="portfolio config of the other rules")
    ap.add_argument("--since", help="first bar to decide again (default: the first signal)")
    ap.add_argument("--json", action="store_true", help="print the result as JSON")
    args = ap.parse_args()

    doc = tomllib.loads(resolve(args.strategy).read_text())
    limits = PortfolioLimits.from_mapping(doc.get("portfolio"))
    take_profit_r = float(doc.get("params", {}).get("take_profit_r", 0))
    universe = {p["binance_symbol"]: p for p in json.loads(UNIVERSE.read_text())["products"]}

    async with AsyncSessionLocal() as s:
        signals = (await s.execute(select(Signal).order_by(Signal.created_at))).scalars().all()
        orders = (await s.execute(select(Order))).scalars().all()
        results = (await s.execute(select(TradeResult))).scalars().all()
    if not signals:
        raise SystemExit("no signals in this database")
    by_signal_order: dict = defaultdict(list)
    for o in orders:
        by_signal_order[o.signal_id].append(o)
    result_of = {r.signal_id: r for r in results}

    def bar_of(sig: Signal) -> datetime:
        pf = sig.pattern_features or {}
        if pf.get("bar_close"):
            return utc(datetime.fromisoformat(pf["bar_close"]))
        c = utc(sig.created_at)
        return c.replace(minute=0, second=0, microsecond=0, hour=c.hour - c.hour % 4)

    bars: dict[datetime, list[Signal]] = defaultdict(list)
    for sig in signals:
        if sig.strategy == STRATEGY:
            bars[bar_of(sig)].append(sig)
    since = utc(datetime.fromisoformat(args.since)) if args.since else min(bars)
    now = datetime.now(timezone.utc)
    symbols = sorted({sig.symbol for sig in signals})

    def taken_here(sig: Signal) -> bool:
        return not (sig.cancelled_reason or "").startswith("portfolio:")

    def real_trade(sig: Signal) -> Trade | None:
        filled = [o for o in by_signal_order.get(sig.id, []) if (o.filled_size or 0) > 0]
        res = result_of.get(sig.id)
        if not filled and res is None:
            return None  # accepted but never filled: the other rules could not do better
        o = filled[0] if filled else None
        cv = float(o.contract_value) if o else 1.0
        qty = float(o.filled_size) * cv if o else float(res.size_filled)
        basis = float(o.price_basis or 0) if o else 0.0
        entry = (float(o.avg_fill_price or o.intended_entry) - basis) if o else float(sig.entry)
        stop = (float(o.stop_loss_price) - basis) if o else float(sig.stop_loss)
        return Trade(
            sig.symbol,
            sig.side,
            qty,
            entry,
            stop,
            bar_of(sig),
            utc(res.closed_at) if res else None,
            float(res.net_pnl) if res else None,
            "real",
            exit_reason=res.exit_reason if res else "open",
            r=float(res.realized_r) if res else None,
        )

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as http:
        start = min(bars) - STEP * (limits.correlation_lookback_bars + 10)
        closes: dict[str, pd.Series] = {}
        source = "Binance"
        for sym in symbols:
            series = await binance_closes(http, sym, start)
            if series is None:
                source = "Delta (Binance unreachable)"
                ds = universe.get(sym, {}).get("delta_symbol")
                series = await delta_closes(http, ds, start) if ds else None
            if series is not None:
                closes[sym] = series

        def mark(sym: str, t: datetime) -> float | None:
            series = closes.get(sym)
            if series is None:
                return None
            cut = series[series.index <= pd.Timestamp(t)]
            return float(cut.iloc[-1]) if len(cut) else None

        async def simulate(sig: Signal, t: datetime, contracts: float) -> Trade:
            spec = universe[sig.symbol]
            cv = float(spec["contract_value"])
            qty = contracts * cv
            long = sig.side == "long"
            entry_b, stop_b = float(sig.entry), float(sig.stop_loss)
            target_b = (
                float(sig.take_profit)
                if sig.take_profit is not None
                else entry_b + (1 if long else -1) * take_profit_r * abs(entry_b - stop_b)
            )
            first = await delta_candles(
                http, spec["delta_symbol"], "1m", int(t.timestamp()), int(t.timestamp()) + 300
            )
            entry_d = float(first[0]["open"]) if first else entry_b
            basis = entry_d - entry_b
            trade = Trade(sig.symbol, sig.side, qty, entry_b, stop_b, t, None, None, "extra")
            trade.basis = basis
            if not take_profit_r and sig.take_profit is None:
                trade.exit_reason = "no target (not simulated)"
                return trade
            ex = await historical_exit(
                http,
                spec["delta_symbol"],
                long,
                Decimal(str(stop_b + basis)),
                Decimal(str(target_b + basis)),
                t,
                now - timedelta(minutes=2),
            )
            if ex is not None:
                exit_d = float(ex.price)
                gross = (1 if long else -1) * (exit_d - entry_d) * qty
                fees = TAKER_FEE * (entry_d + exit_d) * qty
                trade.closed = ex.minute_end
                trade.pnl = gross - fees
                trade.exit_reason = "tp" if ex.purpose.value == "take_profit" else "sl"
                trade.r = trade.pnl / (abs(entry_b - stop_b) * qty) if qty else None
            else:
                trade.exit_reason = "open"
            return trade

        book: list[Trade] = []  # the other rules' trades
        dropped: list[Trade] = []
        decisions: list[dict] = []
        refusals_here: dict[str, int] = defaultdict(int)
        peak3 = None
        pauses = []
        for t in sorted(bars):
            batch = bars[t]
            for sig in batch:
                if not taken_here(sig):
                    refusals_here[(sig.cancelled_reason or "")[10:].split(":")[0]] += 1
            if t < since:
                for sig in batch:
                    if taken_here(sig) and (tr := real_trade(sig)) is not None:
                        book.append(tr)
                continue
            pf = next(((g.pattern_features or {}).get("portfolio") for g in batch), None) or {}
            eq_here = float(pf.get("equity") or 10_000.0)
            delta = sum(tr.value_at(t, mark(tr.symbol, t)) for tr in book if tr.kind == "extra")
            delta -= sum(tr.value_at(t, mark(tr.symbol, t)) for tr in dropped)
            equity = eq_here + delta
            peak3 = max(peak3 or float(pf.get("peak_equity") or equity), equity)
            closed = sorted(
                (tr for tr in book if tr.closed is not None and tr.closed <= t),
                key=lambda tr: tr.closed,
                reverse=True,
            )
            streak = 0
            for tr in closed:
                if (tr.pnl or 0) > 0:
                    break
                streak += 1
            last_loss = closed[0].closed if streak else None
            positions = tuple(
                OpenPosition(
                    tr.symbol, tr.side, tr.qty, tr.entry, mark(tr.symbol, t) or tr.entry, tr.stop
                )
                for tr in book
                if tr.open_at(t)
            )
            cut = {k: v[v.index <= pd.Timestamp(t)] for k, v in closes.items()}
            corr = return_correlations(
                cut, limits.correlation_lookback_bars, limits.correlation_min_bars
            )
            state = PortfolioState(equity, peak3, positions, corr, streak, last_loss, t)
            if state.loss_pause_until(limits) is not None:
                pauses.append((t, streak, state.loss_pause_until(limits)))
            # Live rule outside admit(): no new entry in a symbol that held a position at
            # any time during this bar, even one that has just closed (trend_live.decide).
            busy = {
                tr.symbol
                for tr in book
                if tr.opened < t and (tr.closed is None or tr.closed > t - STEP)
            }
            cands = []
            for sig in batch:
                spec = universe.get(sig.symbol)
                if spec is None:
                    continue
                if sig.symbol in busy:
                    if taken_here(sig) and (tr := real_trade(sig)) is not None:
                        dropped.append(tr)
                        decisions.append(
                            {"bar": t, "trade": tr, "refused_for": "symbol_busy_this_bar"}
                        )
                    continue
                cands.append(
                    Candidate(
                        sig.symbol,
                        sig.side,
                        float(sig.entry),
                        float(sig.stop_loss),
                        float((sig.pattern_features or {}).get("strength") or 0.0),
                        ContractSpec(
                            float(spec["contract_value"]), spec.get("position_size_limit")
                        ),
                        ref=sig,
                    )
                )
            for a in admit(state, cands, limits):
                sig = a.candidate.ref
                here = taken_here(sig)
                if a.accepted and here:
                    if (tr := real_trade(sig)) is not None:
                        book.append(tr)
                elif a.accepted:
                    tr = await simulate(sig, t, a.contracts)
                    book.append(tr)
                    decisions.append(
                        {"bar": t, "trade": tr, "was_refused_for": sig.cancelled_reason}
                    )
                elif here and (tr := real_trade(sig)) is not None:
                    dropped.append(tr)
                    decisions.append({"bar": t, "trade": tr, "refused_for": a.reason})

    extras = [d for d in decisions if "was_refused_for" in d]
    drops = [d for d in decisions if "refused_for" in d]

    def money(x: float | None) -> str:
        return "open" if x is None else f"{x:+,.2f}"

    def latest_value(tr: Trade) -> float:
        if tr.pnl is not None:
            return tr.pnl
        m = mark(tr.symbol, now)
        return tr.value_at(now, m)

    real_closed = sum(float(r.net_pnl) for r in results)
    extra_closed = sum(d["trade"].pnl or 0 for d in extras if d["trade"].pnl is not None)
    extra_open = sum(latest_value(d["trade"]) for d in extras if d["trade"].pnl is None)
    drop_closed = sum(d["trade"].pnl or 0 for d in drops if d["trade"].pnl is not None)
    drop_open = sum(latest_value(d["trade"]) for d in drops if d["trade"].pnl is None)
    net = extra_closed + extra_open - drop_closed - drop_open

    if args.json:
        print(
            json.dumps(
                {
                    "extras": [
                        {
                            "bar": d["bar"].isoformat(),
                            "symbol": d["trade"].symbol,
                            "side": d["trade"].side,
                            "exit": d["trade"].exit_reason,
                            "pnl": d["trade"].pnl,
                            "r": d["trade"].r,
                        }
                        for d in extras
                    ],
                    "dropped": [
                        {
                            "bar": d["bar"].isoformat(),
                            "symbol": d["trade"].symbol,
                            "pnl": d["trade"].pnl,
                            "reason": d["refused_for"],
                        }
                        for d in drops
                    ],
                    "difference": net,
                },
                default=str,
            )
        )
        return

    first, last = min(bars), max(bars)
    print(f"What-if: this bot's candidates {first:%Y-%m-%d %H:%M} -> {last:%Y-%m-%d %H:%M} UTC")
    print(
        f"decided again with {doc.get('name')} (correlated risk "
        f"{limits.max_correlated_risk:.0%}, pause after {limits.loss_pause_after} losses "
        f"for {limits.loss_pause_hours:g}h); correlations from {source} 4h closes"
    )
    print()
    print("Why this bot refused candidates:")
    for k, v in sorted(refusals_here.items(), key=lambda kv: -kv[1]):
        print(f"  {k or 'other':<28} {v}")
    print()
    print(f"Extra trades the other rules would have taken: {len(extras)}")
    for d in extras:
        tr = d["trade"]
        r = "" if tr.r is None else f"{tr.r:+.2f}R"
        closed = "" if tr.closed is None else f"{tr.closed:%m-%d %H:%M}"
        print(
            f"  {d['bar']:%m-%d %H:%M}  {tr.symbol:<9} {tr.side:<5} refused here for "
            f"{(d['was_refused_for'] or '')[10:]:<20} -> {tr.exit_reason:<4} {closed:<11} "
            f"{r:>7}  {money(tr.pnl if tr.pnl is not None else latest_value(tr))}"
            + ("" if tr.pnl is not None else " (unrealized)")
        )
    print()
    print(f"Trades this bot took that the other rules would have refused: {len(drops)}")
    for d in drops:
        tr = d["trade"]
        print(
            f"  {d['bar']:%m-%d %H:%M}  {tr.symbol:<9} {tr.side:<5} refused for "
            f"{d['refused_for']:<20} -> {tr.exit_reason:<4} "
            f"{money(tr.pnl if tr.pnl is not None else latest_value(tr))}"
        )
    print()
    if pauses:
        print("Loss-streak pauses under the other rules:")
        for t, n, until in pauses:
            print(f"  bar {t:%m-%d %H:%M}: {n} losses in a row, paused until {until:%m-%d %H:%M}")
    else:
        print("Loss-streak pause: never triggered.")
    print()
    print(f"This bot, closed trades:              {real_closed:+,.2f}")
    print(f"Extra trades, closed:                 {extra_closed:+,.2f}")
    print(f"Extra trades, still open (marked):    {extra_open:+,.2f}")
    print(f"Removed trades (closed + open):       {-(drop_closed + drop_open):+,.2f}")
    print(f"Difference for the other rules:       {net:+,.2f}")
    print()
    print("Simulated fills: Delta 1-minute trade candles, entry at the first minute's open,")
    print("stop first when both levels are touched in one minute, 0.05% taker fee each side,")
    print("no funding.  Kept trades use their real paper results.")
    if math.isnan(net):
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
