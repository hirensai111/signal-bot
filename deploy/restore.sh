#!/usr/bin/env bash
# Restore a database dump made by the backup service.
#   deploy/restore.sh backups/signal_bot-20261102T000000Z.dump [backups/paper_state-....json]
#   deploy/restore.sh backups/signal_bot_tp-....dump [backups/paper_state_tp-....json]   (second bot)
# Stops the bot, restores into the running postgres service, restores the paper broker
# state if given, then starts the bot again.  The current database is replaced.
set -euo pipefail
dump=${1:?usage: deploy/restore.sh <signal_bot-*.dump | signal_bot_tp-*.dump> [paper_state-*.json]}
state=${2:-}
[ -f "$dump" ] || { echo "no such dump: $dump" >&2; exit 1; }
case "$(basename "$dump")" in
  signal_bot_tp-*) db=signal_bot_tp; service=bot-tp; compose=(docker compose --profile tp) ;;
  *) db=signal_bot; service=bot; compose=(docker compose) ;;
esac
read -r -p "Replace the $db database with $(basename "$dump")? Type RESTORE: " ok
[ "$ok" = "RESTORE" ] || { echo "aborted"; exit 1; }
"${compose[@]}" stop "$service"
"${compose[@]}" exec -T postgres pg_restore -U signalbot -d "$db" --clean --if-exists --no-owner < "$dump"
if [ -n "$state" ]; then
  "${compose[@]}" run --rm --no-deps -v "$(realpath "$state")":/restore.json:ro --entrypoint sh "$service" \
    -c 'cp /restore.json /home/bot/data/paper_state.json'
fi
"${compose[@]}" up -d db-roles "$service"
echo "restored $(basename "$dump"); check the control room and Grafana"
