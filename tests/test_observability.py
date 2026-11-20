"""Ops server, status aggregation, structured logs and dashboard/metric consistency."""

import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer

from signal_bot.db.models import TradeResult
from signal_bot.observability import metrics
from signal_bot.observability.logging import JsonFormatter, bind
from signal_bot.observability.positions import PositionView
from signal_bot.observability.server import OpsServer
from signal_bot.observability.status import StatusService
from tests.harness import Harness

TOKEN = "t0ken-for-tests"


@pytest.fixture
async def ops(tmp_path):
    h = await Harness.create(tmp_path)
    view = PositionView(h.bus)
    status = StatusService(
        environment={"exec_mode": "simulation", "venue": "sim"},
        governor=h.governor,
        bus=h.bus,
        order_store=h.store,
        session_factory=h.raw_factory,
        reconciler=h.reconciler,
        adapter=h.venue,
        position_view=view,
    )
    server = OpsServer(status, h.governor, h.raw_factory, operator_token=TOKEN)
    client = TestClient(TestServer(server.app))
    await client.start_server()
    yield h, client, view
    await client.close()
    await h.close()


def _auth(token=TOKEN):
    return {"Authorization": f"Bearer {token}"}


async def test_metrics_health_and_dashboard(ops):
    _, client, _ = ops
    r = await client.get("/metrics")
    assert r.status == 200 and "signalbot_kill_switch_active" in await r.text()
    assert (await client.get("/healthz")).status == 200
    page = await client.get("/")
    assert page.status == 200 and "Halt trading" in await page.text()
    assert page.headers["X-Frame-Options"] == "DENY"
    assert "frame-ancestors 'none'" in page.headers["Content-Security-Policy"]


async def test_status_reports_trades_positions_and_components(ops):
    h, client, view = ops
    sig = await h.make_signal()
    await h.submit(sig)
    await view.drain()
    body = await (await client.get("/api/status")).json()
    assert body["overall"] in ("healthy", "degraded")
    assert body["positions"][0]["symbol"] == "BTCUSD"
    assert body["orders"][0]["state"] == "filled"
    assert {c["name"] for c in body["components"]} >= {"Risk governor", "Event journal"}
    held = body["protection"][0]
    assert float(held["contract_value"]) > 0
    assert held["mark"] is None  # no trend pipeline here: no 4h close to mark against
    assert body["trades"] == []


async def test_halt_requires_token_and_reason_and_is_audited(ops):
    h, client, _ = ops
    assert (await client.post("/api/risk/halt", json={"reason": "x"})).status == 401
    assert (
        await client.post("/api/risk/halt", json={"reason": "x"}, headers=_auth("nope"))
    ).status == 401
    assert (await client.post("/api/risk/halt", json={"reason": ""}, headers=_auth())).status == 400
    r = await client.post(
        "/api/risk/halt", json={"reason": "spread blew out", "operator": "ana"}, headers=_auth()
    )
    assert r.status == 200
    ready = await client.get("/readyz")
    assert ready.status == 503
    assert "kill_switch_active" in (await ready.json())["reasons"][0]
    audit = (await (await client.get("/api/status")).json())["audit"]
    assert [a["succeeded"] for a in audit] == [True, False, False]
    assert audit[0]["actor"] == "ops:ana"


async def test_resume_requires_typed_confirmation(ops):
    h, client, _ = ops
    await client.post("/api/risk/halt", json={"reason": "drill"}, headers=_auth())
    r = await client.post("/api/risk/resume", json={"reason": "checked"}, headers=_auth())
    assert r.status == 400 and h.governor.gate_open()[0] is False
    r = await client.post(
        "/api/risk/resume", json={"reason": "checked", "confirm": "RESUME"}, headers=_auth()
    )
    assert r.status == 200 and h.governor.gate_open() == (True, None)


async def test_repeated_bad_tokens_throttle_resume_from_that_address(ops):
    _, client, _ = ops
    for _ in range(10):
        await client.post("/api/risk/halt", json={"reason": "xyz"}, headers=_auth("bad"))
    r = await client.post(
        "/api/risk/resume", json={"reason": "xyz", "confirm": "RESUME"}, headers=_auth()
    )
    assert r.status == 429


async def test_control_endpoints_disabled_without_token(tmp_path):
    h = await Harness.create(tmp_path)
    status = StatusService({}, h.governor, h.bus, h.store, h.raw_factory)
    client = TestClient(TestServer(OpsServer(status, h.governor, h.raw_factory, "").app))
    await client.start_server()
    r = await client.post("/api/risk/halt", json={"reason": "abc"}, headers=_auth(""))
    assert r.status == 403
    await client.close()
    await h.close()


async def test_status_survives_database_outage(ops):
    h, client, view = ops
    sig = await h.make_signal()
    await h.submit(sig)
    await view.drain()
    h.factory.down = True
    h.governor.session_factory = h.factory
    body = await (await client.get("/api/status")).json()
    assert "error" in body["risk"]
    assert body["positions"][0]["symbol"] == "BTCUSD"  # visibility kept


def test_json_logs_carry_correlation_context():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "order %s", ("sent",), None)
    with bind(correlation_id="sig-1", order_id="sig_abc"):
        doc = json.loads(JsonFormatter(static={"env": "paper"}).format(record))
    assert doc["msg"] == "order sent"
    assert (
        doc["correlation_id"] == "sig-1" and doc["order_id"] == "sig_abc" and doc["env"] == "paper"
    )


def test_dashboards_and_alerts_only_reference_real_metrics():
    names = set()
    for m in metrics.REGISTRY.collect():
        names |= {m.name, m.name + "_total", m.name + "_bucket"}
    text = (
        Path("deploy/grafana/dashboards/signal-bot.json").read_text()
        + Path("deploy/prometheus/alerts.yml").read_text()
    )
    used = set(re.findall(r"signalbot_[a-z_]+", text))
    # Written by deploy/backup/backup.sh and exported by node-exporter, not by the bot
    # (tests/test_deploy.py checks the script really emits them).
    external = {
        "signalbot_backup_last_success_timestamp_seconds",
        "signalbot_backup_last_size_bytes",
    }
    assert used and used - external <= names, used - external - names


async def test_body_cannot_override_audited_actor_or_ip(ops):
    h, client, _ = ops
    await client.post(
        "/api/risk/halt",
        json={"reason": "drill", "operator": "ana", "actor": "execution_engine", "ip": "10.9.9.9"},
        headers=_auth(),
    )
    audit = (await (await client.get("/api/status")).json())["audit"]
    assert audit[0]["actor"] == "ops:ana"


async def test_failed_guesses_never_block_a_valid_halt(ops):
    h, client, _ = ops
    for _ in range(12):
        await client.post("/api/risk/halt", json={"reason": "xyz"}, headers=_auth("bad"))
    r = await client.post("/api/risk/halt", json={"reason": "real emergency"}, headers=_auth())
    assert r.status == 200 and h.governor.gate_open()[0] is False


async def test_status_lists_closed_trades_newest_first(ops):
    h, client, _ = ops
    t0 = datetime(2026, 11, 20, 8, tzinfo=timezone.utc)
    async with h.raw_factory() as session:
        for i, (pnl, r) in enumerate([(-50, "-1"), (120, "2.4")]):
            session.add(
                TradeResult(
                    signal_id=uuid.uuid4(),
                    venue="paper",
                    symbol="ETHUSD",
                    side="long",
                    intended_entry=Decimal("100"),
                    actual_entry=Decimal("100.5"),
                    exit_price=Decimal("110"),
                    exit_reason="trail",
                    size_requested=Decimal("2"),
                    size_filled=Decimal("2"),
                    gross_pnl=Decimal(pnl),
                    fees=Decimal("1.25"),
                    funding=Decimal("0"),
                    net_pnl=Decimal(pnl),
                    signal_r=Decimal(r),
                    realized_r=Decimal(r),
                    entry_slippage_bps=Decimal("0"),
                    closed_at=t0 + timedelta(hours=i),
                )
            )
        await session.commit()
    body = await (await client.get("/api/status")).json()
    newest, older = body["trades"]
    assert newest["net_pnl"] == 120 and newest["realized_r"] == 2.4
    assert float(newest["entry"]) == 100.5 and newest["exit_reason"] == "trail"
    assert older["net_pnl"] == -50 and older["realized_r"] == -1
    assert newest["closed_at"].startswith("2026-11-20T09:00")


def test_position_mark_is_latest_4h_close_in_venue_prices():
    history = SimpleNamespace(last_close_price=lambda s: {"BTCUSDT": 60000.0}.get(s))
    adapter = SimpleNamespace(binance_symbol=lambda s: {"BTCUSD": "BTCUSDT"}.get(s))
    trend = SimpleNamespace(history=history, portfolio=SimpleNamespace(adapter=adapter))
    status = StatusService({}, None, None, None, None, trend=trend)
    order = SimpleNamespace(symbol="BTCUSD", price_basis=25)
    assert status._mark(order) == 60025.0
    assert status._mark(SimpleNamespace(symbol="XYZUSD", price_basis=None)) is None
    assert StatusService({}, None, None, None, None)._mark(order) is None
