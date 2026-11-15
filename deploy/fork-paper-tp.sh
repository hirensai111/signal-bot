#!/usr/bin/env bash
# First launch of the TP bot, inheriting the main bot's open paper positions.
# Run from the approved release checkout before starting bot-tp for the first time.
set -euo pipefail
cd "$(dirname "$0")/.."
umask 077
mkdir -p backups
exec 9>backups/.paper-fork.lock
flock -n 9 || { echo "Another paper fork is running" >&2; exit 1; }

export SIGNAL_BOT_COMMIT
SIGNAL_BOT_COMMIT=$(git rev-parse HEAD)
compose=(docker compose --profile tp)
if "${compose[@]}" ps --all --services | grep -qx bot-tp; then
  echo "The TP bot already exists. This command is for its first launch only." >&2
  exit 1
fi
if ! "${compose[@]}" ps --status running --services | grep -qx bot; then
  echo "The main paper bot must be running before the fork" >&2
  exit 1
fi

"${compose[@]}" build bot
"${compose[@]}" run --rm --no-deps -T bot python -m signal_bot.ops.preflight --skip-db
"${compose[@]}" run --rm --no-deps -T db-roles
tables=$("${compose[@]}" exec -T postgres psql -XAt -v ON_ERROR_STOP=1 -U signalbot -d signal_bot_tp \
  -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")
if [ "$tables" != 0 ]; then
  echo "The TP database already contains tables. No data was overwritten." >&2
  exit 1
fi
"${compose[@]}" run --rm --no-deps -T --entrypoint sh bot-tp -c 'test ! -e "$PAPER_STATE_PATH"'

snapshot=$(mktemp -d "backups/paper-fork-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")
restart_main() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "Fork interrupted. Starting the main bot. Snapshot directory: $snapshot" >&2
    "${compose[@]}" start bot || true
  fi
}
trap restart_main EXIT

# Stop the writer before copying the two sources of account state.
"${compose[@]}" stop bot
"${compose[@]}" cp bot:/home/bot/data/paper_state.json "$snapshot/paper_state.json"
"${compose[@]}" exec -T postgres pg_dump -Fc -U signalbot -d signal_bot > "$snapshot/signal_bot.dump"
"${compose[@]}" exec -T postgres pg_restore --single-transaction --exit-on-error --no-owner \
  -U signalbot -d signal_bot_tp < "$snapshot/signal_bot.dump"
"${compose[@]}" run --rm --no-deps -T bot-tp python -m signal_bot.ops.fork_tp \
  < "$snapshot/paper_state.json" | tee "$snapshot/fork.json"

"${compose[@]}" up -d
trap - EXIT
"${compose[@]}" up -d --no-deps --force-recreate prometheus alertmanager alloy grafana
echo "$(date -u +%FT%TZ) deployed $SIGNAL_BOT_COMMIT with paper fork $snapshot" >> deploy-history.log
echo "Fork saved at $snapshot. Set COMPOSE_PROFILES=tp in .env for future updates."
