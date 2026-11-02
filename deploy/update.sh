#!/usr/bin/env bash
# Deploy another exact commit to a server set up by bootstrap-ubuntu.sh.
#   cd /opt/signal-bot && sudo deploy/update.sh <git sha>
# Data volumes, .env and backups are kept.  The bot restarts; startup reconciliation
# and the trailing-stop re-derivation restore its state.
set -euo pipefail
commit=${1:?usage: deploy/update.sh <git sha>}
cd "$(dirname "$0")/.."
git fetch --quiet origin
git -c advice.detachedHead=false checkout --quiet "$commit"
export SIGNAL_BOT_COMMIT
SIGNAL_BOT_COMMIT=$(git rev-parse HEAD)
docker compose build bot
docker compose run --rm --no-deps bot python -m signal_bot.ops.preflight --skip-db
docker compose up -d
echo "$(date -u +%FT%TZ) deployed $SIGNAL_BOT_COMMIT" >> deploy-history.log
echo "deployed $SIGNAL_BOT_COMMIT"
