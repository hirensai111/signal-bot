#!/usr/bin/env bash
# Deploy another exact commit to a server set up by bootstrap-ubuntu.sh.
#   cd /opt/signal-bot && sudo deploy/update.sh <git sha>
# Data volumes, .env and backups are kept.  The bot restarts; startup reconciliation
# and the trailing-stop re-derivation restore its state.  With COMPOSE_PROFILES=tp in
# .env the second paper bot is updated and restarted too.
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
if docker compose ps --all --services 2>/dev/null | grep -qx bot-tp \
  && ! docker compose config --services | grep -qx bot-tp; then
  echo "WARNING: the second paper bot (bot-tp) exists but COMPOSE_PROFILES=tp is not in .env," >&2
  echo "         so it was NOT updated. Add COMPOSE_PROFILES=tp to .env and re-run." >&2
fi
echo "$(date -u +%FT%TZ) deployed $SIGNAL_BOT_COMMIT" >> deploy-history.log
echo "deployed $SIGNAL_BOT_COMMIT"
