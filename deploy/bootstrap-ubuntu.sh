#!/usr/bin/env bash
# One-time setup of an Ubuntu 22.04/24.04 server for the PAPER deployment.
#
#   curl -fsSLO https://raw.githubusercontent.com/hirensai111/signal-bot/main/deploy/bootstrap-ubuntu.sh
#   sudo bash bootstrap-ubuntu.sh --commit <git sha to deploy>
#
# Installs Docker, Tailscale and NTP; closes every inbound port except SSH and the
# tailnet; clones the repository at that exact commit into /opt/signal-bot; generates
# the secrets in .env; runs the preflight (Binance and Delta reachability from THIS
# server, clock, disk); starts the stack; and exposes Grafana and the control room on
# your tailnet only (tailscale serve).  Re-running it is safe.
set -euo pipefail

REPO=https://github.com/hirensai111/signal-bot.git
DIR=/opt/signal-bot
COMMIT=""
FORCE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --commit) COMMIT=$2; shift 2 ;;
    --repo) REPO=$2; shift 2 ;;
    --dir) DIR=$2; shift 2 ;;
    --force) FORCE=1; shift ;;   # start even if the preflight fails
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
[ -n "$COMMIT" ] || { echo "--commit <sha> is required: deploy an exact, CI-green commit" >&2; exit 2; }
[ "$(id -u)" -eq 0 ] || { echo "run as root (sudo)" >&2; exit 2; }
say() { printf '\n==> %s\n' "$*"; }

say "System packages, NTP (4h decisions run at bar close: the clock must be right)"
apt-get update -q
apt-get install -yq ca-certificates curl git ufw chrony jq openssl
systemctl enable --now chrony

if ! command -v docker >/dev/null; then
  say "Docker Engine (official apt repository)"
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -q
  apt-get install -yq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
systemctl enable --now docker

if ! command -v tailscale >/dev/null; then
  say "Tailscale"
  curl -fsSL https://tailscale.com/install.sh | sh
fi
if ! tailscale status >/dev/null 2>&1; then
  say "Join your tailnet (open the URL it prints; or set TS_AUTHKEY for unattended setup)"
  if [ -n "${TS_AUTHKEY:-}" ]; then tailscale up --ssh --authkey "$TS_AUTHKEY"; else tailscale up --ssh; fi
fi

say "Firewall: deny inbound except SSH and the tailnet"
ufw default deny incoming
ufw default allow outgoing
ufw allow OpenSSH            # remove once Tailscale SSH works: ufw delete allow OpenSSH
ufw allow in on tailscale0
ufw --force enable

say "Repository at commit $COMMIT"
if [ ! -d "$DIR/.git" ]; then git clone "$REPO" "$DIR"; fi
cd "$DIR"
git fetch --quiet origin
git -c advice.detachedHead=false checkout --quiet "$COMMIT"
mkdir -p backups/metrics

if [ ! -f .env ]; then
  say "Generating .env (secrets are written only to this file, mode 600)"
  cp deploy/paper.env.example .env
  for key in POSTGRES_PASSWORD GRAFANA_DB_PASSWORD GRAFANA_ADMIN_PASSWORD OPERATOR_TOKEN; do
    sed -i "s|^$key=.*|$key=$(openssl rand -hex 24)|" .env
  done
  chmod 600 .env
fi
TS_NAME=$(tailscale status --json | jq -r '.Self.DNSName' | sed 's/\.$//')
sed -i "s|^GRAFANA_ROOT_URL=.*|GRAFANA_ROOT_URL=https://$TS_NAME/|" .env
grep -q '^CONTROL_ROOM_URL=' .env || echo "CONTROL_ROOM_URL=https://$TS_NAME:8443/" >> .env

export SIGNAL_BOT_COMMIT
SIGNAL_BOT_COMMIT=$(git rev-parse HEAD)
say "Building the bot image for $SIGNAL_BOT_COMMIT"
docker compose build bot

say "Preflight from this server"
if ! docker compose run --rm --no-deps bot python -m signal_bot.ops.preflight --skip-db; then
  if [ "$FORCE" -ne 1 ]; then
    echo "Preflight failed: fix the FAIL lines above (e.g. a Binance-restricted region) and re-run." >&2
    exit 1
  fi
  echo "--force: starting anyway" >&2
fi

say "Starting the stack"
docker compose up -d
echo "$(date -u +%FT%TZ) deployed $SIGNAL_BOT_COMMIT" >> deploy-history.log

say "Private access over Tailscale"
tailscale serve --bg --https=443 http://127.0.0.1:3000
tailscale serve --bg --https=8443 http://127.0.0.1:8080

cat <<MSG

Done.  From any device on your tailnet:
  Grafana        https://$TS_NAME/            (user admin, password: GRAFANA_ADMIN_PASSWORD in $DIR/.env)
  Control room   https://$TS_NAME:8443/       (halt/resume need OPERATOR_TOKEN in $DIR/.env)

Next:
  1. Put TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in $DIR/.env, then
     docker compose up -d && docker compose run --rm --no-deps bot python -m signal_bot.ops.preflight --skip-db --telegram-test
  2. Copy $DIR/backups off the server regularly (docs/deployment.md, Backups).
  3. Start the 14-day soak test (docs/deployment.md).
MSG
