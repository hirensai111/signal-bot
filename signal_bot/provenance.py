"""Who/what produced a run: code commit, config fingerprint, strategy version, runtime."""

from __future__ import annotations

import functools
import os
import platform
import subprocess
import sys
from importlib import metadata
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def package_version() -> str:
    try:
        return metadata.version("signal-bot")
    except metadata.PackageNotFoundError:  # pragma: no cover - running from a source tree
        return "0+unknown"


@functools.lru_cache(maxsize=1)
def git_info() -> dict:
    """Commit and dirty flag; ``SIGNAL_BOT_COMMIT`` overrides (set at image build time)."""
    env_commit = os.environ.get("SIGNAL_BOT_COMMIT")
    if env_commit:
        return {"commit": env_commit, "dirty": False, "source": "env"}
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain", "--untracked-files=no"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=5,
                check=True,
            ).stdout.strip()
        )
        return {"commit": commit, "dirty": dirty, "source": "git"}
    except (OSError, subprocess.SubprocessError):
        return {"commit": "unknown", "dirty": None, "source": "unavailable"}


def runtime_info() -> dict:
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "package_version": package_version(),
    }


def provenance(settings_fingerprint: str | None = None, strategy: dict | None = None) -> dict:
    out = {"code": git_info(), "runtime": runtime_info()}
    if settings_fingerprint:
        out["settings_fingerprint"] = settings_fingerprint
    if strategy:
        out["strategy"] = strategy
    return out
