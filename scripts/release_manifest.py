"""Print a JSON manifest tying a release to its code commit and strategy configs."""

import json
from pathlib import Path

from signal_bot.provenance import git_info, package_version
from signal_bot.research.experiment import load_config

configs = sorted(Path("signal_bot/research/strategies").glob("*.toml"))
print(
    json.dumps(
        {
            "version": package_version(),
            "code": git_info(),
            "strategies": [
                {
                    "name": c["name"],
                    "version": c["version"],
                    "config_hash": c["_hash"],
                    "path": c["_path"],
                }
                for c in map(load_config, configs)
            ],
        },
        indent=2,
    )
)
