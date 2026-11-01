"""Ops HTTP server: Prometheus metrics, health probes, control-room UI and operator API.

Endpoints
---------
``GET  /``                 control-room dashboard (static HTML; polls /api/status)
``GET  /metrics``          Prometheus exposition
``GET  /healthz``          liveness (process is serving)
``GET  /readyz``           readiness to trade (503 + reasons when halted/failed)
``GET  /api/status``       aggregated JSON state
``POST /api/risk/halt``    engage the kill switch      (Bearer OPERATOR_TOKEN)
``POST /api/risk/resume``  clear the kill switch       (Bearer OPERATOR_TOKEN + confirmation)

Control endpoints are disabled entirely when no operator token is configured.
Every control attempt — including rejected ones — is written to the audit log.
"""

from __future__ import annotations

import hmac
import json
import logging
import time
from pathlib import Path

from aiohttp import web

from signal_bot.db.models import OperatorAudit
from signal_bot.observability import metrics
from signal_bot.observability.status import StatusService

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
MAX_REASON_LEN = 500
_FAILED_AUTH_WINDOW = 300.0
_FAILED_AUTH_LIMIT = 10


def _json(data, status: int = 200) -> web.Response:
    return web.Response(
        text=json.dumps(data, default=str),
        status=status,
        content_type="application/json",
        headers={"Cache-Control": "no-store"},
    )


class OpsServer:
    def __init__(
        self,
        status: StatusService,
        governor,
        session_factory,
        operator_token: str = "",
        host: str = "127.0.0.1",
        port: int = 8080,
    ) -> None:
        self.status = status
        self.governor = governor
        self.session_factory = session_factory
        self.operator_token = operator_token
        self.host = host
        self.port = port
        self._runner: web.AppRunner | None = None
        self._failed_auth: dict[str | None, list[float]] = {}
        self.app = self.build_app()

    def build_app(self) -> web.Application:
        app = web.Application(middlewares=[self._security_headers], client_max_size=16 * 1024)
        app.add_routes(
            [
                web.get("/", self.dashboard),
                web.get("/metrics", self.metrics),
                web.get("/healthz", self.healthz),
                web.get("/readyz", self.readyz),
                web.get("/api/status", self.api_status),
                web.post("/api/risk/halt", self.halt),
                web.post("/api/risk/resume", self.resume),
            ]
        )
        return app

    @web.middleware
    async def _security_headers(self, request: web.Request, handler):
        response = await handler(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        if response.content_type == "text/html":
            response.headers.setdefault(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self' 'unsafe-inline'; "
                "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
                "font-src https://fonts.gstatic.com; connect-src 'self'; frame-ancestors 'none'",
            )
        return response

    async def start(self) -> None:
        self._runner = web.AppRunner(self.app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info("Ops server listening on http://%s:%d", self.host, self.port)

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    # ------------------------------------------------------------------
    # read endpoints
    # ------------------------------------------------------------------
    async def dashboard(self, request: web.Request) -> web.Response:
        html = (STATIC_DIR / "control_room.html").read_text(encoding="utf-8")
        return web.Response(text=html, content_type="text/html")

    async def metrics(self, request: web.Request) -> web.Response:
        return web.Response(
            body=metrics.render(),
            headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
        )

    async def healthz(self, request: web.Request) -> web.Response:
        return _json({"status": "alive"})

    async def readyz(self, request: web.Request) -> web.Response:
        ok, reasons = self.status.readiness()
        return _json({"ready": ok, "reasons": reasons}, status=200 if ok else 503)

    async def api_status(self, request: web.Request) -> web.Response:
        return _json(await self.status.snapshot())

    # ------------------------------------------------------------------
    # control endpoints
    # ------------------------------------------------------------------
    def _authorised(self, request: web.Request) -> bool:
        if not self.operator_token:
            return False
        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return False
        return hmac.compare_digest(header[7:].encode(), self.operator_token.encode())

    def _locked_out(self, ip: str | None) -> bool:
        """Per-source-IP throttle on failed tokens (never a global lockout)."""
        cutoff = time.monotonic() - _FAILED_AUTH_WINDOW
        recent = [t for t in self._failed_auth.get(ip, []) if t > cutoff]
        self._failed_auth[ip] = recent
        return len(recent) >= _FAILED_AUTH_LIMIT

    async def _audit(self, actor, action, reason, ip, ok) -> None:
        try:
            async with self.session_factory() as session:
                session.add(
                    OperatorAudit(
                        actor=actor[:64],
                        action=action,
                        reason=(reason or "")[:MAX_REASON_LEN],
                        source_ip=ip,
                        succeeded=ok,
                    )
                )
                await session.commit()
        except Exception:
            logger.exception("operator audit write failed")

    async def _parse(self, request: web.Request, action: str):
        ip = request.remote
        if not self.operator_token:
            return None, _json({"error": "control endpoints disabled (no OPERATOR_TOKEN)"}, 403)
        authorised = self._authorised(request)
        # A valid token may always *halt* (the safe direction), even from a throttled
        # address, so failed guesses by someone else can never block an emergency stop.
        if self._locked_out(ip) and not (authorised and action == "halt"):
            return None, _json({"error": "too many failed attempts; try later"}, 429)
        if not authorised:
            self._failed_auth.setdefault(ip, []).append(time.monotonic())
            await self._audit("unauthenticated", action, "bad token", ip, False)
            return None, _json({"error": "unauthorised"}, 401)
        try:
            body = await request.json()
        except (json.JSONDecodeError, ValueError):
            return None, _json({"error": "JSON body required"}, 400)
        if not isinstance(body, dict):
            return None, _json({"error": "JSON object required"}, 400)
        reason = str(body.get("reason") or "").strip()
        actor = str(body.get("operator") or "operator").strip() or "operator"
        if len(reason) < 3:
            return None, _json({"error": "a reason (>= 3 chars) is required"}, 400)
        parsed = {
            "reason": reason[:MAX_REASON_LEN],
            "actor": f"ops:{actor[:40]}",
            "ip": ip,
            "confirm": body.get("confirm"),
        }
        return parsed, None

    async def halt(self, request: web.Request) -> web.Response:
        parsed, error = await self._parse(request, "halt")
        if error is not None:
            return error
        try:
            await self.governor.set_kill_switch(
                parsed["reason"], actor=parsed["actor"], source_ip=parsed["ip"]
            )
        except Exception as exc:
            # The kill switch is engaged in memory even if persisting failed.
            logger.exception("halt persisted partially")
            return _json({"halted": True, "persisted": False, "error": str(exc)}, 202)
        return _json({"halted": True, "persisted": True})

    async def resume(self, request: web.Request) -> web.Response:
        parsed, error = await self._parse(request, "resume")
        if error is not None:
            return error
        if parsed.get("confirm") != "RESUME":
            await self._audit(
                parsed["actor"], "resume", "no typed confirmation", parsed["ip"], False
            )
            return _json({"error": 'resume requires {"confirm": "RESUME"}'}, 400)
        breakers = self.governor.breakers
        await self.governor.clear_kill_switch(
            actor=parsed["actor"], reason=parsed["reason"], source_ip=parsed["ip"]
        )
        return _json({"resumed": True, "open_circuit_breakers": breakers})
