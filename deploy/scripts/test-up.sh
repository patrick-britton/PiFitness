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
  "provision test checkout (venv, Python requirements, frontend build)"
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
# Dump-before-stop order (D3, not amended): the dump is taken while PRD
# writers are still running, so the test DB snapshots PRD at dump time;
# writers are quiesced in Step 3 before the clone, and PRD writes between
# the dump and the quiesce are not captured. Preserve the dump as a
# disaster-recovery artifact; test-down never restores PRD from it.
if [[ ! -s "$dump_path" ]]; then
  echo "ERROR: dump is missing or empty: $dump_path" >&2
  exit 1
fi
if ! pg_restore --list "$dump_path" >/dev/null 2>&1; then
  echo "ERROR: dump archive is invalid: $dump_path" >&2
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
# pm2 describe exits 0 for a registered process even when it is stopped, so
# probe the status text itself: pifitness-next is "online" only when running.
pm2_nextprod_active="unknown"
if pm2 describe pifitness-next >/dev/null 2>&1 && pm2 describe pifitness-next 2>/dev/null | grep -qi 'online'; then
  pm2_nextprod_active="online"
fi

current_phase="initial"
write_state
echo "  test state durable: $STATE_FILE (phase=$current_phase)"

echo "Step 3: stop PRD services and timers"
sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
sudo systemctl stop pifitness_agent.timer 2>/dev/null || true
sudo systemctl stop pifitness-hardware-health.timer 2>/dev/null || true
pm2 stop pifitness-next 2>/dev/null || true

echo "  verify PRD writers are quiesced before cloning the database"
fastapi_state="$(systemctl is-active pifitness-fastapi.service 2>/dev/null || echo unknown)"
agent_timer_state="$(systemctl is-active pifitness_agent.timer 2>/dev/null || echo unknown)"
hardware_health_timer_state="$(systemctl is-active pifitness-hardware-health.timer 2>/dev/null || echo unknown)"
pm2_next_state="stopped"
if pm2 describe pifitness-next >/dev/null 2>&1 && pm2 describe pifitness-next 2>/dev/null | grep -qi 'online'; then
  pm2_next_state="online"
fi
if [[ "$fastapi_state" == "active" || "$agent_timer_state" == "active" \
      || "$hardware_health_timer_state" == "active" || "$pm2_next_state" == "online" ]]; then
  echo "ERROR: PRD writers still active after stop (fastapi=$fastapi_state agent.timer=$agent_timer_state hardware-health.timer=$hardware_health_timer_state pm2-next=$pm2_next_state)" >&2
  exit 1
fi
echo "  PRD quiesced: fastapi=$fastapi_state agent.timer=$agent_timer_state hardware-health.timer=$hardware_health_timer_state pm2-next=$pm2_next_state"
current_phase="prd-stopped"
write_state

echo "Step 4: create test DB"

# If the TEST DB already exists, inspect its marker before touching it:
# drop/recreate only when it is marked for test use (pifitness.env='test');
# refuse if it is unmarked or marked otherwise.
if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -tAc \
     "SELECT 1 FROM pg_database WHERE datname='$TEST_DB';" 2>/dev/null | grep -q 1; then
  existing_marker="$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$TEST_DB" -tAc \
    "SELECT current_setting('pifitness.env', true);" 2>/dev/null | tr -d '[:space:]')"
  if [[ "$existing_marker" == "test" ]]; then
    echo "  $TEST_DB exists and is marked test; dropping and recreating"
    # Never suppress a failed DROP: a failed drop stops here instead of
    # continuing onto a database that may not have been replaced.
    if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
        -c "DROP DATABASE \"$TEST_DB\""; then
      echo "ERROR: failed to drop $TEST_DB" >&2
      exit 1
    fi
  else
    echo "ERROR: refusing to drop/recreate $TEST_DB (marker='${existing_marker:-<none>}')" >&2
    exit 1
  fi
fi
if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
     -c "CREATE DATABASE \"$TEST_DB\" TEMPLATE \"$PRD_DB\";"; then
  echo "  $TEST_DB created from template $PRD_DB"
else
  echo "  template clone failed; falling back to pg_dump/pg_restore"
  dropdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" --if-exists "$TEST_DB"
  createdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$TEST_DB"
  # TimescaleDB: prepare the empty database for hypertable restore.
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$TEST_DB" \
    -c "SELECT timescaledb_pre_restore();" >/dev/null
  if ! pg_restore -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$TEST_DB" "$dump_path"; then
    echo "ERROR: pg_restore of $dump_path into $TEST_DB failed" >&2
    exit 1
  fi
  # TimescaleDB: finalize the hypertable restore.
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$TEST_DB" \
    -c "SELECT timescaledb_post_restore();" >/dev/null
fi

# Stamp the test marker (D8) only after successful creation/restore, then
# connect to that exact host/port/database and verify the marker.
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
  -c "ALTER DATABASE \"$TEST_DB\" SET pifitness.env = 'test';" >/dev/null
current_phase="test-db-created"
marker="$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$TEST_DB" -tAc \
  "SELECT current_setting('pifitness.env', true);" 2>/dev/null | tr -d '[:space:]')"
if [[ "$marker" != "test" ]]; then
  echo "ERROR: $TEST_DB is not marked for test use (marker='${marker:-<none>}')" >&2
  exit 1
fi
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

echo "Step 8: provision test checkout (venv, Python requirements, frontend build)"
# Remove any inherited artifacts from the PRD checkout before provisioning.
rm -rf "$TEST_DIR/venv" "$TEST_DIR/frontend/pifitness/node_modules" "$TEST_DIR/frontend/pifitness/.next" "$TEST_DIR/.env"
# Create the backend venv inside the test checkout.
python3 -m venv "$TEST_DIR/venv"
# Install current runtime requirements (Phase 5 package-sync approach).
"$TEST_DIR/venv/bin/pip install" -r "$TEST_DIR/backend/requirements.txt"
# Install the backend editable package from the checked-out tree.
"$TEST_DIR/venv/bin/pip install" -e "$TEST_DIR/backend"
# Confirm pifitness resolves inside the test checkout backend package.
installed_pifitness="$("$TEST_DIR/venv/bin/python" -c "import pifitness; print(pifitness.__file__)")" || {
  echo "ERROR: failed to import pifitness in test checkout venv" >&2
  exit 1
}
case "$installed_pifitness" in
  "$TEST_DIR"/backend/pifitness/*)
    echo "  pifitness resolves to $installed_pifitness" ;;
  *)
    echo "ERROR: pifitness does not resolve to test checkout backend: $installed_pifitness" >&2
    exit 1 ;;
esac
# Install frontend dependencies reproducibly from the committed lockfile.
(
  cd "$TEST_DIR/frontend/pifitness"
  npm ci
)
# Build the frontend with test build-time environment. NEXT_PUBLIC_API_URL
# remains unset unless the application contract requires a same-origin value.
(
  cd "$TEST_DIR/frontend/pifitness"
  NEXT_PUBLIC_APP_ENV=test npm run build
)
current_phase="checkout-provisioned"
write_state

echo "Step 9: start test units"
sudo systemctl start pifitness-fastapi-test.service 2>/dev/null || true
if [[ -f "$TEST_DIR/deploy/scripts/ecosystem.config.js" ]]; then
  pm2 start "$TEST_DIR/deploy/scripts/ecosystem.config.js" 2>/dev/null || true
fi
current_phase="test-units-started"
write_state

current_phase="complete"
write_state
echo "test-up complete."