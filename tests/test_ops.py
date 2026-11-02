"""Deployment preflight and soak-report logic (network replaced by fakes)."""

from __future__ import annotations

import asyncio

from signal_bot.ops import preflight, soak


async def test_a_hanging_network_check_fails_instead_of_hanging():
    async def never():
        await asyncio.sleep(3600)

    result = await preflight.bounded(never(), "binance websocket", seconds=0.05)
    assert result.status == "FAIL" and "no answer" in result.detail


def test_preflight_flags_live_mode_and_testnet_market_data(monkeypatch):
    monkeypatch.setattr(preflight.settings, "exec_mode", "live")
    monkeypatch.setattr(preflight.settings, "binance_testnet", True)
    checks = {c.name: c.status for c in preflight.check_settings()}
    assert checks["exec mode"] == "FAIL" and checks["market data feed"] == "FAIL"


class FakeProm:
    """Answers the soak report's queries from a table keyed by query fragments."""

    def __init__(self, answers):
        self.answers = answers

    def _find(self, expr):
        for key, value in self.answers.items():
            if key in expr:
                return value
        return None

    async def scalar(self, expr):
        value = self._find(expr)
        return None if isinstance(value, dict) else value

    async def by(self, expr, label):
        value = self._find(expr)
        return value if isinstance(value, dict) else {}


def healthy(days=14):
    return {
        "avg_over_time(up": 0.999,
        "changes(process_start_time_seconds": 1,
        "signalbot_trend_symbol_tradable == 1": {"BTCUSDT": 1, "ETHUSDT": 1},
        'outcome="appended"': {"BTCUSDT": days * 6, "ETHUSDT": days * 6 - 2},
        "signalbot_trend_batches_total": 80,
        "max_over_time(signalbot_positions_unprotected": 0,
        "signalbot_protective_order_failures_total": 0,
        'outcome=~"rejected|unknown"': 0,
        'outcome="moved"': 12,
        "signalbot_reconciliation_failures_total": 0,
        "time() - signalbot_reconciliation_last_success": 120,
        "time() - signalbot_backup_last_success": 3600,
        "max by (alertname)": {"BotRestarted": 1},
        'ALERTS{alertname="Watchdog"': 1,
        'node_filesystem_avail_bytes{mountpoint="/"} offset': 60e9,
        "min_over_time(node_filesystem_avail_bytes": 55e9,
        'node_filesystem_avail_bytes{mountpoint="/"}': 58e9,
        'node_filesystem_size_bytes{mountpoint="/"}': 80e9,
    }


async def test_soak_passes_a_healthy_fortnight():
    crit, facts = await soak.collect(FakeProm(healthy()), 14)
    assert all(c.passed for c in crit), [c for c in crit if not c.passed]
    assert facts["bars_expected_per_symbol"] == 84
    assert facts["days_until_disk_full"] == round(58e9 / (2e9 / 14))
    report = soak.render(crit, facts, 14)
    assert "**PASSED**" in report


async def test_soak_fails_on_missed_4h_closes_and_unprotected_positions():
    answers = healthy()
    answers['outcome="appended"'] = {"BTCUSDT": 84, "ETHUSDT": 40}
    answers["max_over_time(signalbot_positions_unprotected"] = 1
    crit, _ = await soak.collect(FakeProm(answers), 14)
    failed = {c.name for c in crit if not c.passed}
    assert "Every tradable symbol processed >= 95% of its 4h closes" in failed
    assert "No position ever without a stop" in failed
