#!/usr/bin/env bash
set -euo pipefail

PRD_DIR="${PRD_DIR:-$HOME/PiFitness}"
TEST_DIR="${TEST_DIR:-$HOME/PiFitness-test}"
STATE_FILE="${STATE_FILE:-$HOME/.pifitness-test-state}"
PRD_DB="${PRD_DB:-pifitness}"
TEST_DB="${TEST_DB:-pifitness_test}"
DB_USER="${DB_USER:-dev}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-15432}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1/api/health}"

keep_db=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-db)
      keep_db=1
      shift
      ;;
    --dry-run)
      echo "test-down dry-run plan:"
      echo "  stop test units"
      if [[ $keep_db -eq 1 ]]; then
        echo "  keep test DB ($TEST_DB)"
      else
        echo "  drop $TEST_DB (marker must be 'test')"
      fi
      echo "  remove $TEST_DIR"
      echo "  restart PRD services and timers"
      echo "  call $HEALTH_URL"
      echo "  remove $STATE_FILE"
      exit 0
      ;;
    -h|--help)
      echo "Usage: $0 [--keep-db] [--dry-run]"
      exit 0
      ;;
    *)
      echo "unexpected argument: $1" >&2
      exit 2
      ;;
  esac
done

echo "Step 1: stop test units"
sudo systemctl stop pifitness-fastapi-test.service 2>/dev/null || true
pm2 delete pifitness-next-test 2>/dev/null || true

echo "Step 2: drop test DB"
if [[ $keep_db -eq 1 ]]; then
  echo "  --keep-db set; skipping drop"
else
  if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -tAc \
       "SELECT 1 FROM pg_database WHERE datname='$TEST_DB';" 2>/dev/null | grep -q 1; then
    marker="$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$TEST_DB" -tAc \
      "SELECT current_setting('pifitness.env', true);" 2>/dev/null | tr -d '[:space:]')"
    if [[ "$marker" != "test" ]]; then
      echo "ERROR: refusing to drop $TEST_DB (marker='${marker:-<none>}')" >&2
      exit 1
    fi
    dropdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" --if-exists "$TEST_DB"
  else
    echo "  $TEST_DB absent; nothing to drop"
  fi
fi

echo "Step 3: remove test checkout"
rm -rf "$TEST_DIR"

echo "Step 4: restart PRD services and timers"
sudo systemctl start pifitness-fastapi.service 2>/dev/null || true
sudo systemctl start pifitness_agent.timer 2>/dev/null || true
sudo systemctl start pifitness-hardware-health.timer 2>/dev/null || true

echo "Step 5: health check"
if command -v curl >/dev/null 2>&1; then
  curl -fsS "$HEALTH_URL" || echo "  health check failed (non-fatal)"
else
  echo "  curl not available; skipping health check"
fi

echo "Step 6: remove state file"
rm -f "$STATE_FILE"

echo "test-down complete."
if [[ "$PRD_DB" == "$TEST_DB" ]]; then
  echo "ERROR: PRD and TEST DB names must differ" >&2
  exit 1
fi