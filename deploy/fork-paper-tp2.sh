#!/usr/bin/env bash
# First launch of the third paper bot (bot-tp2) as an exact copy of the second (bot-tp).
#
# The third bot trades the second bot's strategy (same entries, fixed stop, +1.5R target)
# and differs only in two account rules: a 5% correlated-risk limit and a 48-hour pause
# after 5 losing trades in a row.  Its exits are identical, so no replay is needed: the
# second bot's database and paper checkpoint are copied as they are, and both bots then
# continue from the same trades, open positions and balance.
#
#   cd /opt/signal-bot && sudo bash deploy/fork-paper-tp2.sh
#
# Run it from the approved release checkout, with bot-tp running and bot-tp2 never started.
set -euo pipefail
cd "$(dirname "$0")/.."
umask 077
mkdir -p backups
exec 9>backups/.paper-fork.lock
flock -n 9 || { echo "Another paper fork is running" >&2; exit 1; }

export SIGNAL_BOT_COMMIT
SIGNAL_BOT_COMMIT=$(git rev-parse HEAD)
compose=(docker compose --profile tp --profile tp2)
if "${compose[@]}" ps --all --services | grep -qx bot-tp2; then
  echo "The third paper bot already exists. This command is for its first launch only." >&2
  exit 1
fi
if ! "${compose[@]}" ps --status running --services | grep -qx bot-tp; then
  echo "The second paper bot (bot-tp) must be running before the fork" >&2
  exit 1
fi

"${compose[@]}" build bot
"${compose[@]}" run --rm --no-deps -T bot python -m signal_bot.ops.preflight --skip-db
"${compose[@]}" run --rm --no-deps -T db-roles
tables=$("${compose[@]}" exec -T postgres psql -XAt -v ON_ERROR_STOP=1 -U signalbot -d signal_bot_tp2 \
  -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")
if [ "$tables" != 0 ]; then
  echo "The signal_bot_tp2 database already contains tables. No data was overwritten." >&2
  exit 1
fi
"${compose[@]}" run --rm --no-deps -T --entrypoint sh bot-tp2 -c 'test ! -e "$PAPER_STATE_PATH"'

snapshot=$(mktemp -d "backups/paper-fork-tp2-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")
restart_tp() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "Fork interrupted. Starting the second bot again. Snapshot directory: $snapshot" >&2
    "${compose[@]}" start bot-tp || true
  fi
}
trap restart_tp EXIT

# Stop the writer so the database and the paper checkpoint are captured together.
"${compose[@]}" stop bot-tp
"${compose[@]}" cp bot-tp:/home/bot/data/paper_state.json "$snapshot/paper_state.json"
"${compose[@]}" exec -T postgres pg_dump -Fc -U signalbot -d signal_bot_tp > "$snapshot/signal_bot_tp.dump"
"${compose[@]}" exec -T postgres pg_restore --single-transaction --exit-on-error --no-owner \
  -U signalbot -d signal_bot_tp2 < "$snapshot/signal_bot_tp.dump"
"${compose[@]}" run --rm --no-deps -T --entrypoint sh bot-tp2 \
  -c 'cat > "$PAPER_STATE_PATH" && test -s "$PAPER_STATE_PATH"' < "$snapshot/paper_state.json"
counts=$("${compose[@]}" exec -T postgres psql -XAt -U signalbot -d signal_bot_tp2 -c \
  "SELECT (SELECT count(*) FROM orders) || ' orders, ' || (SELECT count(*) FROM trade_results) || ' closed trades'")
printf '{"forked_at": "%s", "source": "bot-tp (signal_bot_tp)", "commit": "%s", "copied": "%s"}\n' \
  "$(date -u +%FT%TZ)" "$SIGNAL_BOT_COMMIT" "$counts" | tee "$snapshot/fork.json"

"${compose[@]}" up -d
trap - EXIT
"${compose[@]}" up -d --no-deps --force-recreate prometheus alertmanager alloy grafana
echo "$(date -u +%FT%TZ) deployed $SIGNAL_BOT_COMMIT with paper fork tp2 $snapshot" >> deploy-history.log
echo "Third bot started from a copy of the second ($counts). Snapshot: $snapshot"
echo "Next: set COMPOSE_PROFILES=tp,tp2 in .env, and publish its control room:"
echo "  sudo tailscale serve --bg --https=8445 http://127.0.0.1:8082"
