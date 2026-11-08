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
BACKUP_DIR="${BACKUP_DIR:-$HOME/Documents/DB_Backups}"

dry_run=0
ref=""

usage() {
  echo "Usage: $0 <ref> [--dry-run]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      dry_run=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "$ref" ]]; then
        ref="$1"
        shift
      else
        echo "unexpected argument: $1" >&2
        exit 2
      fi
      ;;
  esac
done

if [[ -z "$ref" ]]; then
  usage >&2
  exit 2
fi

if [[ "$PRD_DB" == "$TEST_DB" ]]; then
  echo "ERROR: PRD and TEST DB names must differ" >&2
  exit 1
fi

if [[ -f "$STATE_FILE" ]]; then
  echo "ERROR: already in test mode ($STATE_FILE exists)" >&2
  exit 1
fi

if [[ -d "$PRD_DIR/.git" ]]; then
  if [[ -n "$(git -C "$PRD_DIR" status --porcelain)" ]]; then
    echo "ERROR: PRD working tree is dirty" >&2
    exit 1
  fi
fi

timestamp="$(date +%Y%m%d_%H%M%S)"
dump_path="$BACKUP_DIR/pifitness_prd_${timestamp}.dump"

steps=(
  "record PRD commit"
  "dump PRD DB to $dump_path"
  "stop PRD services and timers"
  "create $TEST_DB from PRD (template clone, fallback pg_dump/pg_restore)"
  "apply pending migrations to $TEST_DB"
  "check out $ref into $TEST_DIR (own venv and build)"
  "start test units"
  "write $STATE_FILE"
)

if [[ $dry_run -eq 1 ]]; then
  echo "test-up dry-run plan:"
  echo "  ref=$ref"
  echo "  PRD_DIR=$PRD_DIR"
  echo "  TEST_DIR=$TEST_DIR"
  echo "  PRD_DB=$PRD_DB TEST_DB=$TEST_DB"
  i=1
  for step in "${steps[@]}"; do
    echo "  $i. $step"
    i=$((i + 1))
  done
  exit 0
fi

# Preflight: require and validate the external test env file before touching PRD.
TEST_ENV_FILE="${HOME}/.config/pifitness/test.env"
if [[ ! -f "$TEST_ENV_FILE" ]]; then
  echo "ERROR: test env file not found: $TEST_ENV_FILE" >&2
  echo "  Copy the tracked test env example in deploy/ to $TEST_ENV_FILE and fill in secrets." >&2
  exit 1
fi
# shellcheck disable=SC1090
set -a
. "$TEST_ENV_FILE"
set +a

if [[ "${APP_ENV:-}" != "test" ]]; then
  echo "ERROR: APP_ENV must be 'test' in $TEST_ENV_FILE (got '${APP_ENV:-}')" >&2
  exit 1
fi
if [[ "${PG_DB:-}" != "$TEST_DB" ]]; then
  echo "ERROR: PG_DB must be '$TEST_DB' in $TEST_ENV_FILE (got '${PG_DB:-}')" >&2
  exit 1
fi
if [[ "${PG_HOST:-}" != "$DB_HOST" || "${PG_PORT:-}" != "$DB_PORT" ]]; then
  echo "ERROR: PG_HOST/PG_PORT must match $DB_HOST:$DB_PORT in $TEST_ENV_FILE" >&2
  exit 1
fi
for integration in SPOTIFY_ENABLED GARMIN_ENABLED PIRATE_ENABLED; do
  if [[ "${!integration:-false}" == "true" ]]; then
    echo "ERROR: $integration must be disabled in test mode in $TEST_ENV_FILE" >&2
    exit 1
  fi
done
chmod 0600 "$TEST_ENV_FILE"
echo "  test env file validated: $TEST_ENV_FILE"

echo "Step 1: record PRD commit"
prd_commit="$(git -C "$PRD_DIR" rev-parse HEAD)"
echo "  PRD commit: $prd_commit"

echo "Step 2: dump PRD DB"
mkdir -p "$BACKUP_DIR"
if ! pg_dump -Fc -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$PRD_DB" -f "$dump_path"; then
  echo "ERROR: dump failed" >&2
  exit 1
fi

echo "Step 3: stop PRD services and timers"
sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
sudo systemctl stop pifitness_agent.timer 2>/dev/null || true
sudo systemctl stop pifitness-hardware-health.timer 2>/dev/null || true

echo "Step 4: create test DB"
if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
     -c "DROP DATABASE IF EXISTS \"$TEST_DB\";" >/dev/null 2>&1; then
  true
fi
if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
     -c "CREATE DATABASE \"$TEST_DB\" TEMPLATE \"$PRD_DB\";"; then
  echo "  template clone failed; falling back to pg_restore"
  dropdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" --if-exists "$TEST_DB" 2>/dev/null || true
  createdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$TEST_DB"
  pg_restore -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$TEST_DB" "$dump_path"
fi

# Stamp the test marker (D8).
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
  -c "ALTER DATABASE \"$TEST_DB\" SET pifitness.env = 'test';" >/dev/null

echo "Step 5: apply pending migrations"
if [[ -x "$PRD_DIR/deploy/scripts/run_migrations.sh" ]]; then
  DB_NAME="$TEST_DB" "$PRD_DIR/deploy/scripts/run_migrations.sh"
else
  echo "  no migration runner found; skipping"
fi

echo "Step 6: check out ref into test dir"
rm -rf "$TEST_DIR"
git clone "$PRD_DIR" "$TEST_DIR"
git -C "$TEST_DIR" checkout "$ref"

# Copy the validated external test env into the test checkout so the test
# unit's EnvironmentFile exists before the unit is started.
cp "$TEST_ENV_FILE" "$TEST_DIR/.env.test"
chmod 0600 "$TEST_DIR/.env.test"
echo "  test env copied to $TEST_DIR/.env.test"

echo "Step 7: start test units"
sudo systemctl start pifitness-fastapi-test.service 2>/dev/null || true
if [[ -f "$TEST_DIR/deploy/scripts/ecosystem.config.js" ]]; then
  pm2 start "$TEST_DIR/deploy/scripts/ecosystem.config.js" 2>/dev/null || true
fi

echo "Step 8: write state file"
printf 'PRD_COMMIT=%s\nDUMP_PATH=%s\nREF=%s\n' "$prd_commit" "$dump_path" "$ref" > "$STATE_FILE"

echo "test-up complete."