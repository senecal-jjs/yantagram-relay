#!/bin/sh
set -e
DB_PATH="${DB_PATH:-/data/relay.db}"
DATA_DIR="$(dirname "$DB_PATH")"
mkdir -p "$DATA_DIR"

# The JVM runs as an unprivileged user. Fly mounts volumes root-owned, so fix the
# owner first, then drop privileges for everything below.
if [ "$(id -u)" = "0" ]; then
  chown -R relay:relay "$DATA_DIR"
  exec setpriv --reuid=relay --regid=relay --init-groups "$0" "$@"
fi

if [ -n "$LITESTREAM_REPLICA_URL" ]; then
  # A new volume (or a lost one) starts from the latest replica.
  litestream restore -config /etc/litestream.yml -if-db-not-exists -if-replica-exists "$DB_PATH"
  # Litestream runs the relay as its child and replicates until it exits.
  exec litestream replicate -config /etc/litestream.yml -exec "$*"
fi
echo "LITESTREAM_REPLICA_URL is not set: the database is NOT replicated" >&2
exec "$@"
