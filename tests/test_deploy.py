"""Deployment invariants: dashboards and alerts use real metrics; compose stays paper-only and private."""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from signal_bot.observability import metrics

ROOT = Path(__file__).resolve().parent.parent
DASHBOARDS = sorted((ROOT / "deploy/grafana/dashboards").glob("*.json"))
ALERTS = ROOT / "deploy/prometheus/alerts.yml"
COMPOSE = ROOT / "docker-compose.yml"
# Metrics from other exporters (node-exporter textfile written by deploy/backup/backup.sh).
EXTERNAL = {"signalbot_backup_last_success_timestamp_seconds", "signalbot_backup_last_size_bytes"}


def _exported_names() -> set[str]:
    names = set()
    for metric in metrics.REGISTRY.collect():
        for sample in metric.samples:
            names.add(sample.name)
        names.add(metric.name)
    return names


def _referenced(text: str) -> set[str]:
    return set(re.findall(r"\bsignalbot_[a-z0-9_]+", text))


def test_every_dashboard_and_alert_metric_is_exported_by_the_bot():
    exported = _exported_names()
    texts = [p.read_text() for p in DASHBOARDS] + [ALERTS.read_text()]
    missing = {n for t in texts for n in _referenced(t)} - exported - EXTERNAL
    # Labelled metrics with no children yet export no samples: check the family names too.
    families = {m.name for m in metrics.REGISTRY.collect()}
    missing = {n for n in missing if not any(n == f or n.startswith(f + "_") for f in families)}
    assert not missing, (
        f"dashboards/alerts reference metrics the bot does not export: {sorted(missing)}"
    )


def test_backup_metric_names_match_the_backup_script():
    script = (ROOT / "deploy/backup/backup.sh").read_text()
    assert EXTERNAL <= _referenced(script)


def test_dashboards_are_valid_and_use_provisioned_datasources():
    provisioned = yaml.safe_load(
        (ROOT / "deploy/grafana/provisioning/datasources/datasources.yml").read_text()
    )
    uids = {d["uid"] for d in provisioned["datasources"]}
    for path in DASHBOARDS:
        dash = json.loads(path.read_text())
        assert dash["uid"] and dash["title"]
        ids = [p["id"] for p in dash["panels"]]
        assert len(ids) == len(set(ids)), f"duplicate panel ids in {path.name}"
        for panel in dash["panels"]:
            for target in panel.get("targets", []):
                uid = (target.get("datasource") or panel.get("datasource") or {}).get("uid")
                assert uid in uids, (
                    f"{path.name}: panel {panel['title']!r} uses unknown datasource {uid}"
                )


def test_overview_dashboard_is_generated_from_its_source():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "build_overview", ROOT / "deploy/grafana/build_overview.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    committed = json.loads(
        (ROOT / "deploy/grafana/dashboards/signal-bot-overview.json").read_text()
    )
    assert committed == json.loads(json.dumps(mod.build())), (
        "run: python deploy/grafana/build_overview.py"
    )


def test_open_risk_panels_show_the_entry_limit_next_to_actual_exposure():
    dash = json.loads((ROOT / "deploy/grafana/dashboards/signal-bot-overview.json").read_text())
    panel = next(p for p in dash["panels"] if p["title"].startswith("Open risk to stops"))
    exprs = [t["expr"] for t in panel["targets"]]
    assert "signalbot_portfolio_open_risk_ratio" in exprs
    assert 'signalbot_portfolio_limit{limit="max_open_risk"}' in exprs
    assert "new entries only" in panel["description"]


def test_compose_is_paper_only_and_private():
    compose = yaml.safe_load(COMPOSE.read_text())
    services = compose["services"]
    bot = services["bot"]["environment"]
    assert bot["EXEC_MODE"] == "paper" and bot["PAPER_ONLY"] == "true"
    for name, svc in services.items():
        for port in svc.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), f"{name} publishes {port} beyond localhost"
        if name not in ("db-roles",):
            assert svc.get("restart") == "unless-stopped", (
                f"{name} would not come back after a reboot"
            )
    named = set(compose["volumes"])
    assert {
        "pgdata",
        "botdata",
        "prometheus-data",
        "grafana-data",
        "loki-data",
        "alertmanager-data",
    } <= named
    assert services["grafana"]["environment"]["GF_AUTH_ANONYMOUS_ENABLED"] == "false"


def test_alertmanager_template_placeholders_are_all_rendered():
    tmpl = (ROOT / "deploy/alertmanager/alertmanager.yml.tmpl").read_text()
    entry = (ROOT / "deploy/alertmanager/entrypoint.sh").read_text()
    for placeholder in set(re.findall(r"__[A-Z_]+__", tmpl)):
        assert placeholder in entry, f"{placeholder} is never substituted"


def test_paper_only_setting_refuses_live():
    from signal_bot.config import Settings

    s = Settings(
        _env_file=None, exec_mode="live", paper_only=True, delta_api_key="k", delta_api_secret="s"
    )
    assert any("PAPER_ONLY" in p for p in s.validate_for_startup())
