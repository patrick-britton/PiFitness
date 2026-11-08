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

# Canonicalize and validate a directory path before any destructive use.
# Refuses: empty paths, "/", and symlinks. Returns the canonical absolute path.
canonicalize_path() {
  local path="$1"
  if [[ -z "$path" ]]; then
    echo "ERROR: empty path" >&2
    return 1
  fi
  if [[ "$path" == "/" ]]; then
    echo "ERROR: path is root: $path" >&2
    return 1
  fi
  if [[ -L "$path" ]]; then
    echo "ERROR: path is symlink: $path" >&2
    return 1
  fi
  realpath -m "$path"
}

dry_run=0
ref=""

usage() {
  echo "Usage: $0  [--dry-run]"
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

# Canonicalize and validate paths before any destructive use.
PRD_DIR="$(canonicalize_path "$PRD_DIR")" || exit 1
TEST_DIR="$(canonicalize_path "$TEST_DIR")" || exit 1

if [[ "$TEST_DIR" == "$PRD_DIR" ]]; then
  echo "ERROR: TEST_DIR must not equal PRD_DIR: $TEST_DIR" >&2
  exit 1
fi
case "$PRD_DIR" in
  "$TEST_DIR"/*)
    echo "ERROR: TEST_DIR is an ancestor of PRD_DIR: $TEST_DIR" >&2
    exit 1
    ;;
esac

timestamp="$(date +%Y%m%d_%H%M%S)"
dump_path="$BACKUP_DIR/pifitness_prd_${timestamp}.dump"

steps=(
  "record PRD commit"
  "dump PRD DB to $dump_path"
  "write durable test state ($STATE_FILE)"
  "stop PRD services and timers"
  "create $TEST_DB from PRD (template clone, fallback pg_dump/pg_restore)"
  "apply pending migrations to $TEST_DB"
  "fetch $ref from origin and resolve to full SHA"
  "check out resolved SHA into $TEST_DIR (own venv and build)"
  "start test units"
  "write final test state"
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

if [[ -f "$STATE_FILE" ]]; then
  echo "ERROR: already in test mode ($STATE_FILE exists)" >&2
  exit 1
fi

echo "Step 1: record PRD commit"
prd_commit="$(git -C "$PRD_DIR" rev-parse HEAD)"
echo "  PRD commit: $prd_commit"

echo "Step 2: dump PRD DB"
mkdir -p "$BACKUP_DIR"
if ! pg_dump -Fc -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$PRD_DB" -f "$dump_path"; then
  echo "ERROR: dump failed" >&2
  exit 1
fi

# --- Durable test state (D3) --------------------------------------------
# current_phase records the last completed major transition. The state file is
# written atomically (temp file plus rename, mode 0600, no secrets) after
# every transition and before any PRD service, timer, or PM2 process is
# stopped, so a crash or Ctrl+C leaves enough information for test-down to
# decide what to restore. Never store secrets (PG_PASSWORD, tokens, etc.)
# in the state file.
current_phase="started"

write_state() {
  local tmp="${STATE_FILE}.tmp.$$"
  # umask 077 makes the temp file 0600; mv keeps that mode on the target.
  ( umask 077
    printf 'PHASE=%s\n' "$current_phase" > "$tmp"
    printf 'PRD_COMMIT=%s\n' "${prd_commit:-}" >> "$tmp"
    printf 'REF=%s\n' "${ref:-}" >> "$tmp"
    printf 'RESOLVED_SHA=%s\n' "${resolved_sha:-}" >> "$tmp"
    printf 'PRD_DB=%s\n' "$PRD_DB" >> "$tmp"
    printf 'TEST_DB=%s\n' "$TEST_DB" >> "$tmp"
    printf 'DB_HOST=%s\n' "$DB_HOST" >> "$tmp"
    printf 'DB_PORT=%s\n' "$DB_PORT" >> "$tmp"
    printf 'DUMP_PATH=%s\n' "${dump_path:-}" >> "$tmp"
    printf 'PRD_DIR=%s\n' "$PRD_DIR" >> "$tmp"
    printf 'TEST_DIR=%s\n' "$TEST_DIR" >> "$tmp"
    printf 'FASTAPI_ACTIVE=%s\n' "${fastapi_active:-unknown}" >> "$tmp"
    printf 'AGENT_TIMER_ACTIVE=%s\n' "${agent_timer_active:-unknown}" >> "$tmp"
    printf 'HARDWARE_HEALTH_TIMER_ACTIVE=%s\n' "${hardware_health_timer_active:-unknown}" >> "$tmp"
    printf 'PM2_NEXTPROD_ACTIVE=%s\n' "${pm2_nextprod_active:-unknown}" >> "$tmp"
  )
  mv -f "$tmp" "$STATE_FILE"
}

# Crash/Ctrl+C safety: persist the current phase so test-down can decide what
# was already done and what still needs restoring.
trap 'write_state' INT TERM

# Capture the initial active state of PRD services, timers, and PM2 BEFORE
# anything is stopped, then persist it durably (atomic write, mode 0600).
fastapi_active="$(systemctl is-active pifitness-fastapi.service 2>/dev/null || echo unknown)"
agent_timer_active="$(systemctl is-active pifitness_agent.timer 2>/dev/null || echo unknown)"
hardware_health_timer_active="$(systemctl is-active pifitness-hardware-health.timer 2>/dev/null || echo unknown)"
pm2_nextprod_active="$(pm2 describe pifitness-next >/dev/null 2>&1 && echo online || echo unknown)"

current_phase="initial"
write_state
echo "  test state durable: $STATE_FILE (phase=$current_phase)"

echo "Step 3: stop PRD services and timers"
sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
sudo systemctl stop pifitness_agent.timer 2>/dev/null || true
sudo systemctl stop pifitness-hardware-health.timer 2>/dev/null || true
current_phase="prd-stopped"
write_state

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
current_phase="test-db-created"
write_state

echo "Step 5: apply pending migrations"
if [[ -x "$PRD_DIR/deploy/scripts/run_migrations.sh" ]]; then
  DB_NAME="$TEST_DB" "$PRD_DIR/deploy/scripts/run_migrations.sh"
else
  echo "  no migration runner found; skipping"
fi
current_phase="migrations-applied"
write_state

echo "Step 6: fetch $ref from origin and resolve to full SHA"
git -C "$PRD_DIR" fetch origin "$ref"
resolved_sha="$(git -C "$PRD_DIR" rev-parse FETCH_HEAD)"
echo "  resolved $ref to $resolved_sha"
current_phase="ref-resolved"
write_state

echo "Step 7: check out resolved SHA into test dir"
rm -rf "$TEST_DIR"
git clone "$PRD_DIR" "$TEST_DIR"
git -C "$TEST_DIR" checkout "$resolved_sha"

# Copy the validated external test env into the test checkout so the test
# unit's EnvironmentFile exists before the unit is started.
cp "$TEST_ENV_FILE" "$TEST_DIR/.env.test"
chmod 0600 "$TEST_DIR/.env.test"
echo "  test env copied to $TEST_DIR/.env.test"
current_phase="checkout-done"
write_state

echo "Step 8: start test units"
sudo systemctl start pifitness-fastapi-test.service 2>/dev/null || true
if [[ -f "$TEST_DIR/deploy/scripts/ecosystem.config.js" ]]; then
  pm2 start "$TEST_DIR/deploy/scripts/ecosystem.config.js" 2>/dev/null || true
fi
current_phase="test-units-started"
write_state

current_phase="complete"
write_state
echo "test-up complete."