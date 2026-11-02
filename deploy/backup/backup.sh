#!/bin/sh
# Daily PostgreSQL dump (custom format, restorable with deploy/restore.sh) plus the
# paper broker's state file.  Keeps BACKUP_RETENTION_DAYS days in /backups (./backups
# on the host).  Writes a node-exporter textfile metric so Prometheus can alert on a
# stale backup.
set -eu
mkdir -p /backups/metrics
while true; do
  until pg_isready -q; do sleep 5; done
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  tmp="/backups/.signal_bot-$stamp.dump.partial"
  if pg_dump -Fc -f "$tmp"; then
    mv "$tmp" "/backups/signal_bot-$stamp.dump"
    if [ -f /botdata/paper_state.json ]; then
      cp /botdata/paper_state.json "/backups/paper_state-$stamp.json"
    fi
    find /backups -maxdepth 1 -name 'signal_bot-*.dump' -mtime +"$BACKUP_RETENTION_DAYS" -delete
    find /backups -maxdepth 1 -name 'paper_state-*.json' -mtime +"$BACKUP_RETENTION_DAYS" -delete
    size=$(wc -c < "/backups/signal_bot-$stamp.dump")
    printf '# HELP signalbot_backup_last_success_timestamp_seconds Unix time of the last good database backup\n# TYPE signalbot_backup_last_success_timestamp_seconds gauge\nsignalbot_backup_last_success_timestamp_seconds %s\n# HELP signalbot_backup_last_size_bytes Size of the last database backup\n# TYPE signalbot_backup_last_size_bytes gauge\nsignalbot_backup_last_size_bytes %s\n' \
      "$(date -u +%s)" "$size" > /backups/metrics/.backup.prom.tmp
    mv /backups/metrics/.backup.prom.tmp /backups/metrics/backup.prom
    echo "backup ok: signal_bot-$stamp.dump ($size bytes)"
  else
    rm -f "$tmp"
    echo "backup FAILED at $stamp; retrying in 1h" >&2
    sleep 3600
    continue
  fi
  sleep "$BACKUP_INTERVAL_SECONDS"
done
