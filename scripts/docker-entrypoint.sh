#!/usr/bin/env sh
set -eu

DB_PATH="${DB_PATH:-/app/data/renfe.db}"
CACHE_FILE="${CACHE_FILE:-/app/data/cache/flotaLD.latest.json}"
BOOTSTRAP_SQLITE_PATH="${BOOTSTRAP_SQLITE_PATH:-}"
BOOTSTRAP_CACHE_PATH="${BOOTSTRAP_CACHE_PATH:-}"
AUTO_MIGRATE_PG_ON_START="${AUTO_MIGRATE_PG_ON_START:-0}"

mkdir -p "$(dirname "$DB_PATH")"
mkdir -p "$(dirname "$CACHE_FILE")"

if [ -n "$BOOTSTRAP_SQLITE_PATH" ] && [ ! -s "$DB_PATH" ] && [ -s "$BOOTSTRAP_SQLITE_PATH" ]; then
  cp "$BOOTSTRAP_SQLITE_PATH" "$DB_PATH"
  echo "[docker-bootstrap] SQLite importada desde $BOOTSTRAP_SQLITE_PATH"
fi

if [ -n "$BOOTSTRAP_CACHE_PATH" ] && [ ! -s "$CACHE_FILE" ] && [ -s "$BOOTSTRAP_CACHE_PATH" ]; then
  cp "$BOOTSTRAP_CACHE_PATH" "$CACHE_FILE"
  echo "[docker-bootstrap] Cache importada desde $BOOTSTRAP_CACHE_PATH"
fi

if [ "$AUTO_MIGRATE_PG_ON_START" = "1" ]; then
  export SQLITE_PATH="${SQLITE_PATH:-$DB_PATH}"

  if [ -n "${POSTGRES_URL:-}" ] && [ -s "$SQLITE_PATH" ]; then
    echo "[docker-bootstrap] Migracion automatica SQLite -> Postgres iniciada"
    if bun run migrate:pg; then
      echo "[docker-bootstrap] Migracion automatica completada"
    else
      echo "[docker-bootstrap] Migracion automatica fallida, continuando con el arranque"
    fi
  else
    echo "[docker-bootstrap] Migracion automatica omitida (POSTGRES_URL o SQLITE_PATH no disponibles)"
  fi
fi

exec bun run start
