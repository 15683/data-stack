#!/bin/sh
set -eu
umask 077
stamp=$(date -u +%Y%m%dT%H%M%SZ)
# Remove old files before allocating a new dump, preserving the latest completed
# copy of each database even when it is older than the retention threshold.
for database in airflow_db metabase_db dwh_db; do
  latest=$(find /backups -name "*-${database}.dump" -type f | sort | tail -1)
  find /backups -name "*-${database}.dump" -type f -mtime "+$RETENTION_DAYS" |
    while IFS= read -r file; do
      if [ "$file" != "$latest" ]; then rm -- "$file"; fi
    done
done
# A manifest is useful only while all three files in its set remain available.
for manifest in /backups/*.sha256; do
  [ -f "$manifest" ] || continue
  prefix=${manifest%.sha256}
  for database in airflow_db metabase_db dwh_db; do
    if [ ! -f "${prefix}-${database}.dump" ]; then rm -- "$manifest"; break; fi
  done
done
find /backups -type f -name '*.partial' -mtime +1 -delete
free_kb=$(df -Pk /backups | awk 'NR==2 {print $4}')
if [ "$free_kb" -lt "$MIN_FREE_KB" ]; then
  echo "Backup aborted: free space ${free_kb} KiB below minimum ${MIN_FREE_KB} KiB" >&2
  exit 1
fi
for database in airflow_db metabase_db dwh_db; do
  if [ "$database" = dwh_db ]; then
    host=postgres-dwh
    export PGPASSWORD="$POSTGRES_DWH_PASSWORD"
  else
    host=postgres-main
    export PGPASSWORD="$POSTGRES_MAIN_PASSWORD"
  fi
  # After a VM reboot the scheduled Job can start before PostgreSQL is ready.
  attempt=0
  until pg_isready -h "$host" -U postgres >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 60 ] || { echo "PostgreSQL unavailable: $host" >&2; exit 1; }
    sleep 2
  done
  pg_dump -h "$host" -U postgres -Fc "$database" > "/backups/${stamp}-${database}.dump.partial"
  pg_restore --list "/backups/${stamp}-${database}.dump.partial" >/dev/null
  mv "/backups/${stamp}-${database}.dump.partial" "/backups/${stamp}-${database}.dump"
done
cd /backups
sha256sum "${stamp}-"*.dump > "${stamp}.sha256"
echo "Backup complete: $stamp"
