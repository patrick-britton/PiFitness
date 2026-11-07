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

dry_run=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) dry_run=1; shift ;;
    -h|--help) echo "Usage: $0 [--dry-run]"; exit 0 ;;
    *) echo "unexpected arg: $1" >&2; exit 2 ;;
  esac
done

if [[ "$PRD_DB" == "$TEST_DB" ]]; then
  echo "ERROR: PRD and TEST DB names match" >&2
  exit 1
fi

if [[ $dry_run -eq 1 ]]; then
  echo "test-down dry-run plan:"
  echo "  stop test units"
  echo "  drop $TEST_DB"
  echo "  remove $TEST_DIR"
  echo "  restart PRD services and timers"
  echo "  remove $STATE_FILE"
  exit 0
fi

echo "Step 1: stop test units"
sudo systemctl stop pifitness-fastapi-test.service 2>/dev/null || true
pm2 delete pifitness-next-test 2>/dev/null || true

echo "Step 2: drop test DB"
dropdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" --if-exists "$TEST_DB" 2>/dev/null || true

echo "Step 3: remove test checkout"
rm -rf "$TEST_DIR"

echo "Step 4: restart PRD services and timers"
sudo systemctl start pifitness-fastapi.service 2>/dev/null || true
sudo systemctl start pifitness_agent.timer pifitness-hardware-health.timer 2>/dev/null || true

echo "Step 5: remove state file"
rm -f "$STATE_FILE"

echo "test-down complete."