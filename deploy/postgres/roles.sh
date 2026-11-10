#!/bin/sh
# Idempotent: a read-only role for Grafana's trade-performance panels, and the database
# of the optional second paper bot (signal_bot_tp, compose profile "tp").
# Runs on every `docker compose up` (one-shot service), so existing databases get it too.
set -eu
until pg_isready -q; do sleep 1; done
psql -v ON_ERROR_STOP=1 -v pw="$GRAFANA_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE grafana_ro LOGIN PASSWORD %L', :'pw')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') \gexec
ALTER ROLE grafana_ro WITH LOGIN PASSWORD :'pw';
ALTER ROLE grafana_ro SET default_transaction_read_only = on;
ALTER ROLE grafana_ro SET statement_timeout = '15s';
GRANT CONNECT ON DATABASE signal_bot TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
-- tables the bot creates later (it owns them as signalbot)
ALTER DEFAULT PRIVILEGES FOR ROLE signalbot IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
SELECT 'CREATE DATABASE signal_bot_tp OWNER signalbot'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'signal_bot_tp') \gexec
GRANT CONNECT ON DATABASE signal_bot_tp TO grafana_ro;
\connect signal_bot_tp
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE signalbot IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
SQL
echo "grafana_ro role ready (read-only); database signal_bot_tp ready"
