"""Deployment preflight: can this server run the paper bot correctly?

    docker compose run --rm bot python -m signal_bot.ops.preflight
    python -m signal_bot.ops.preflight --telegram-test     # also sends a test message

Checks, each PASS / WARN / FAIL (exit code 1 on any FAIL):

* paper-only settings (EXEC_MODE=paper, PAPER_ONLY, production market data);
* Binance USD-M public market data from THIS server: REST 4h klines and the 4h
  kline WebSocket.  HTTP 451 means Binance restricts the hosting location; pick a
  region where its terms allow access rather than routing around the restriction;
* clock skew against Binance (or Delta) server time: 4h decisions run at bar close;
* Delta public API: live perpetuals for the strategy universe and a non-empty order
  book (the paper broker fills on Delta's books);
* database connectivity, free disk, Telegram configuration.
"""

from __future__ import annotations

import argparse
import asyncio
import email.utils
import json
import shutil
import sys
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path

import aiohttp

from signal_bot.config import settings
from signal_bot.delta.client import ENDPOINTS

BINANCE_REST = "https://fapi.binance.com"
BINANCE_WS = "wss://fstream.binance.com/market/ws/btcusdt@kline_4h"
TIMEOUT = aiohttp.ClientTimeout(total=15)


@dataclass
class Check:
    name: str
    status: str  # PASS | WARN | FAIL
    detail: str


def _universe() -> list[str]:
    try:
        with open(settings.strategy_config, "rb") as fh:
            doc = tomllib.load(fh)
    except OSError:
        return list(settings.symbols)
    return list(doc.get("universe", {}).get("symbols") or settings.symbols)


def check_settings() -> list[Check]:
    out = []
    mode_ok = settings.exec_mode == "paper"
    out.append(Check("exec mode", "PASS" if mode_ok else "FAIL", f"EXEC_MODE={settings.exec_mode}"))
    out.append(
        Check(
            "paper-only lock",
            "PASS" if settings.paper_only else "WARN",
            "PAPER_ONLY=true" if settings.paper_only else "PAPER_ONLY is not set (compose sets it)",
        )
    )
    out.append(
        Check(
            "market data feed",
            "FAIL" if settings.binance_testnet else "PASS",
            "BINANCE_TESTNET=true is synthetic data"
            if settings.binance_testnet
            else "production (public)",
        )
    )
    out.append(
        Check(
            "operator token",
            "PASS" if settings.operator_token else "WARN",
            "set (halt/resume enabled)"
            if settings.operator_token
            else "OPERATOR_TOKEN empty: the control room cannot halt",
        )
    )
    problems = settings.validate_for_startup()
    out.append(
        Check("startup validation", "FAIL" if problems else "PASS", "; ".join(problems) or "ok")
    )
    return out


async def bounded(coro, name: str, seconds: float = 25.0):
    """Every network check gets a hard deadline: a blocked endpoint must not hang preflight."""
    try:
        return await asyncio.wait_for(coro, seconds)
    except TimeoutError:
        return Check(name, "FAIL", f"no answer within {seconds:.0f}s (blocked or unreachable)")


async def check_binance_rest(session: aiohttp.ClientSession) -> Check:
    try:
        async with session.get(
            f"{BINANCE_REST}/fapi/v1/klines",
            params={"symbol": "BTCUSDT", "interval": "4h", "limit": "2"},
        ) as resp:
            body = await resp.text()
            if resp.status == 451:
                return Check(
                    "binance rest",
                    "FAIL",
                    "HTTP 451: Binance restricts this hosting location. Choose a region its "
                    "terms allow; do not route around the restriction.",
                )
            if resp.status != 200:
                return Check("binance rest", "FAIL", f"HTTP {resp.status}: {body[:120]}")
            rows = json.loads(body)
            return Check("binance rest", "PASS", f"4h klines ok (last close {rows[-1][4]})")
    except Exception as exc:
        return Check("binance rest", "FAIL", f"{type(exc).__name__}: {exc}")


async def check_binance_ws(session: aiohttp.ClientSession) -> Check:
    try:
        async with session.ws_connect(BINANCE_WS, heartbeat=10) as ws:
            msg = await ws.receive(timeout=20)
            if msg.type == aiohttp.WSMsgType.TEXT and '"k"' in msg.data:
                return Check("binance websocket", "PASS", "4h kline stream delivering")
            return Check("binance websocket", "FAIL", f"unexpected message {msg.type}")
    except Exception as exc:
        return Check("binance websocket", "FAIL", f"{type(exc).__name__}: {exc}")


async def check_clock(session: aiohttp.ClientSession) -> Check:
    # (url, parser, precision in seconds): Binance gives ms; an HTTP Date header whole seconds.
    sources = [
        (f"{BINANCE_REST}/fapi/v1/time", lambda r, b: json.loads(b)["serverTime"] / 1000, 0.0),
        (
            f"{ENDPOINTS[settings.delta_mode]['rest']}/v2/products?page_size=1",
            lambda r, b: email.utils.parsedate_to_datetime(r.headers["Date"]).timestamp(),
            1.0,
        ),
    ]
    for url, parse, precision in sources:
        try:
            t0 = time.time()
            async with session.get(url) as resp:
                body = await resp.text()
                if resp.status != 200:
                    continue
                t1 = time.time()
                server = parse(resp, body)
        except Exception:
            continue
        skew = abs(server - (t0 + t1) / 2)
        status = "PASS" if skew <= 1 + precision else "WARN" if skew <= 5 + precision else "FAIL"
        return Check(
            "clock skew", status, f"{skew:.2f}s vs {url.split('/')[2]} (enable NTP if large)"
        )
    return Check("clock skew", "WARN", "no server time source reachable")


async def check_delta(session: aiohttp.ClientSession) -> list[Check]:
    base = ENDPOINTS[settings.delta_mode]["rest"]
    out = []
    try:
        async with session.get(
            f"{base}/v2/products",
            params={"contract_types": "perpetual_futures", "states": "live", "page_size": "500"},
        ) as resp:
            products = (await resp.json()).get("result", [])
        live = {p["symbol"] for p in products}
        universe = _universe()
        tradable = [s for s in universe if s.replace("USDT", "USD") in live]
        missing = [s for s in universe if s not in tradable]
        status = "PASS" if tradable else "FAIL"
        detail = f"{len(tradable)}/{len(universe)} tradable on Delta {settings.delta_mode}"
        if missing:
            detail += f" (not listed: {', '.join(missing)})"
        out.append(Check("delta products", status, detail))
        if tradable:
            sym = tradable[0].replace("USDT", "USD")
            async with session.get(f"{base}/v2/l2orderbook/{sym}") as resp:
                book = (await resp.json()).get("result", {})
            ok = bool(book.get("buy")) and bool(book.get("sell"))
            out.append(
                Check(
                    "delta order book",
                    "PASS" if ok else "FAIL",
                    f"{sym}: {'bids and asks' if ok else 'empty'}",
                )
            )
    except Exception as exc:
        out.append(Check("delta products", "FAIL", f"{type(exc).__name__}: {exc}"))
    return out


async def check_database() -> Check:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as conn:
            version = (await conn.execute(text("SELECT version()"))).scalar_one()
        return Check("database", "PASS", version.split(",")[0])
    except Exception as exc:
        return Check("database", "FAIL", f"{type(exc).__name__}: {str(exc)[:120]}")
    finally:
        await engine.dispose()


def check_disk(path: str) -> Check:
    p = Path(path)
    while not p.exists() and p != p.parent:
        p = p.parent
    usage = shutil.disk_usage(p)
    free_gb = usage.free / 1e9
    status = "PASS" if free_gb >= 10 else "WARN" if free_gb >= 3 else "FAIL"
    return Check("disk", status, f"{free_gb:.1f} GB free of {usage.total / 1e9:.1f} GB at {p}")


async def check_telegram(session: aiohttp.ClientSession, send: bool) -> Check:
    token, chat = settings.telegram_bot_token, settings.telegram_chat_id
    if not (token and chat):
        return Check("telegram", "WARN", "not configured: alerts will not reach your phone")
    try:
        async with session.get(f"https://api.telegram.org/bot{token}/getMe") as resp:
            me = await resp.json()
        if not me.get("ok"):
            return Check("telegram", "FAIL", f"token rejected: {me.get('description')}")
        if send:
            async with session.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat, "text": "Signal Bot preflight: Telegram alerts work."},
            ) as resp:
                sent = await resp.json()
            if not sent.get("ok"):
                return Check("telegram", "FAIL", f"send failed: {sent.get('description')}")
            return Check("telegram", "PASS", f"@{me['result']['username']}: test message sent")
        return Check(
            "telegram", "PASS", f"@{me['result']['username']} (use --telegram-test to send one)"
        )
    except Exception as exc:
        return Check("telegram", "FAIL", f"{type(exc).__name__}: {exc}")


async def run(skip_db: bool = False, telegram_test: bool = False) -> list[Check]:
    checks = check_settings()
    async with aiohttp.ClientSession(timeout=TIMEOUT, trust_env=True) as session:
        checks.append(await bounded(check_binance_rest(session), "binance rest"))
        checks.append(await bounded(check_binance_ws(session), "binance websocket"))
        checks.append(await bounded(check_clock(session), "clock skew"))
        delta = await bounded(check_delta(session), "delta products", 40)
        checks += delta if isinstance(delta, list) else [delta]
        checks.append(await bounded(check_telegram(session, telegram_test), "telegram"))
    if not skip_db:
        checks.append(await check_database())
    checks.append(check_disk(str(Path(settings.paper_state_path).parent)))
    return checks


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--skip-db", action="store_true")
    ap.add_argument("--telegram-test", action="store_true")
    args = ap.parse_args()
    checks = asyncio.run(run(args.skip_db, args.telegram_test))
    width = max(len(c.name) for c in checks)
    for c in checks:
        print(f"{c.status:4}  {c.name:<{width}}  {c.detail}")
    failed = [c for c in checks if c.status == "FAIL"]
    print(
        f"\n{'NOT READY' if failed else 'READY'}: {len(failed)} failed, "
        f"{sum(c.status == 'WARN' for c in checks)} warnings"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
