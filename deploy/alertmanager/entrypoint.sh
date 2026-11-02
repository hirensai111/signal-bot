#!/bin/sh
# Renders alertmanager.yml from the template using environment variables, so the
# Telegram token never sits in a committed file.  Without Telegram settings the
# stack still starts and alerts stay visible in Alertmanager and Grafana.
set -eu
src=/etc/alertmanager-src
out=/tmp/alertmanager.yml
receiver=blackhole
if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_CHAT_ID:-}" ]; then
  umask 077
  printf '%s' "$TELEGRAM_BOT_TOKEN" > /tmp/telegram_token
  receiver=telegram
else
  echo "WARNING: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set: alerts will not be sent" >&2
fi
deadman_receiver=blackhole
if [ -n "${DEADMANS_SWITCH_URL:-}" ]; then
  deadman_receiver=deadman
fi
sed -e "s|__RECEIVER__|$receiver|g" \
    -e "s|__CHAT_ID__|${TELEGRAM_CHAT_ID:-0}|g" \
    -e "s|__API_URL__|${TELEGRAM_API_URL:-https://api.telegram.org}|g" \
    -e "s|__DEADMAN_RECEIVER__|$deadman_receiver|g" \
    -e "s|__DEADMAN_URL__|${DEADMANS_SWITCH_URL:-http://127.0.0.1:9/unused}|g" \
    "$src/alertmanager.yml.tmpl" > "$out"
if [ "${RENDER_ONLY:-}" = "1" ]; then  # CI: print the rendered config and stop
  cat "$out"
  exit 0
fi
exec /bin/alertmanager --config.file="$out" --storage.path=/alertmanager --web.listen-address=:9093
