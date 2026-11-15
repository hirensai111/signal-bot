#!/bin/sh
# Daily PostgreSQL dump (custom format, restorable with deploy/restore.sh) plus the
# paper broker's state file.  The second and third paper bots (compose profiles "tp" and
# "tp2") are backed up the same way, as signal_bot_tp[2]-*.dump and paper_state_tp[2]-*.json,
# once their database has tables.  Keeps BACKUP_RETENTION_DAYS days in /backups (./backups
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
    # Second and third paper bots: only once a database exists and its bot created tables.
    for suffix in tp tp2; do
      db="signal_bot_$suffix"
      if [ "$(psql -d "$db" -tAc "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'" 2>/dev/null || echo 0)" -gt 0 ]; then
        part="/backups/.${db}-$stamp.dump.partial"
        if pg_dump -Fc -d "$db" -f "$part"; then
          mv "$part" "/backups/${db}-$stamp.dump"
        else
          rm -f "$part"
          echo "backup of $db FAILED at $stamp" >&2
        fi
        if [ -f "/botdata-$suffix/paper_state.json" ]; then
          cp "/botdata-$suffix/paper_state.json" "/backups/paper_state_${suffix}-$stamp.json"
        fi
      fi
    done
    for pattern in 'signal_bot-*.dump' 'paper_state-*.json' 'signal_bot_tp-*.dump' 'paper_state_tp-*.json' \
                   'signal_bot_tp2-*.dump' 'paper_state_tp2-*.json'; do
      find /backups -maxdepth 1 -name "$pattern" -mtime +"$BACKUP_RETENTION_DAYS" -delete
    done
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
