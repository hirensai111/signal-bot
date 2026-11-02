"""Structured JSON logging with correlation context.

``bind(signal_id=..., order_id=..., correlation_id=...)`` stores identifiers in
context variables; every log line emitted inside that context (including from
other modules and awaited coroutines) carries them, so one trade can be traced
end to end with a single query on ``correlation_id``.
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

_CONTEXT_FIELDS = ("correlation_id", "signal_id", "order_id", "event_id", "symbol")
_ctx: dict[str, contextvars.ContextVar[str | None]] = {
    name: contextvars.ContextVar(name, default=None) for name in _CONTEXT_FIELDS
}

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


@contextlib.contextmanager
def bind(**fields: Any):
    tokens = []
    for name, value in fields.items():
        if name in _ctx and value is not None:
            tokens.append((_ctx[name], _ctx[name].set(str(value))))
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


def current_context() -> dict[str, str]:
    return {name: var.get() for name, var in _ctx.items() if var.get() is not None}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str = "signal-bot", static: dict[str, Any] | None = None):
        super().__init__()
        self.service = service
        self.static = static or {}

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "service": self.service,
            **self.static,
            **current_context(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                doc[key] = value
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


class ContextTextFormatter(logging.Formatter):
    """Human-readable format that still shows the correlation identifiers."""

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        ctx = current_context()
        if not ctx:
            return base
        return base + " | " + " ".join(f"{k}={v}" for k, v in ctx.items())


def configure_logging(level: str = "INFO", fmt: str = "json", static: dict | None = None) -> None:
    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter(static=static))
    else:
        handler.setFormatter(
            ContextTextFormatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
        )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    for noisy in ("aiohttp.access", "sqlalchemy.engine.Engine", "httpx", "telegram"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
