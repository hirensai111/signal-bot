#!/usr/bin/env bash
# Restore a database dump made by the backup service.
#   deploy/restore.sh backups/signal_bot-20261102T000000Z.dump [backups/paper_state-....json]
# Stops the bot, restores into the running postgres service, restores the paper broker
# state if given, then starts the bot again.  The current database is replaced.
set -euo pipefail
dump=${1:?usage: deploy/restore.sh <signal_bot-*.dump> [paper_state-*.json]}
state=${2:-}
[ -f "$dump" ] || { echo "no such dump: $dump" >&2; exit 1; }
read -r -p "Replace the signal_bot database with $(basename "$dump")? Type RESTORE: " ok
[ "$ok" = "RESTORE" ] || { echo "aborted"; exit 1; }
docker compose stop bot
docker compose exec -T postgres pg_restore -U signalbot -d signal_bot --clean --if-exists --no-owner < "$dump"
if [ -n "$state" ]; then
  docker compose run --rm --no-deps -v "$(realpath "$state")":/restore.json:ro --entrypoint sh bot \
    -c 'cp /restore.json /home/bot/data/paper_state.json'
fi
docker compose up -d db-roles bot
echo "restored $(basename "$dump"); check the control room and Grafana"
