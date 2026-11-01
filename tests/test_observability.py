"""Ops server, status aggregation, structured logs and dashboard/metric consistency."""

import json
import logging
import re
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

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
    assert used and used <= names, used - names


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
