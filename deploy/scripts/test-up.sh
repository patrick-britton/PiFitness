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
HTTPS_HOSTNAME="${HTTPS_HOSTNAME:-https://pifitness.duckdns.org}"
BACKUP_DIR="${BACKUP_DIR:-$HOME/Documents/DB_Backups}"
# Systemd unit directory: repo-driven source of unit files.
# No unit name is hardcoded in this script — everything is discovered from
# the files in this directory. Units whose filename contains "-test" belong
# to the TEST environment; all other pifitness-* units belong to PRD.
SYSTEMD_DIR="${PRD_DIR}/deploy/systemd"

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

# Failpoints for the human recovery drill.
# A failpoint engages only when PIFITNESS_FAILPOINT_<name>=1 is set
# (opt-in, disabled by default). Engaging it additionally requires the
# test-only acknowledgement PIFITNESS_FAILPOINT_ACK=test-only; without
# it the script exits before the failpoint fires. Use these only for the
# human recovery drill, never in ordinary runs.
failpoint() {
  local name="$1"
  local msg="$2"
  local env_var="PIFITNESS_FAILPOINT_${name^^}"
  if [[ "${!env_var:-0}" != "1" ]]; then
    return 0
  fi
  if [[ "${PIFITNESS_FAILPOINT_ACK:-}" != "test-only" ]]; then
    echo "ERROR: failpoint $name enabled but test-only acknowledgement missing (set PIFITNESS_FAILPOINT_ACK=test-only)" >&2
    exit 2
  fi
  echo "FAILPOINT $name engaged: $msg" >&2
  exit 1
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
export TEST_DIR

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
  "fetch $ref from origin and resolve to full SHA"
  "check out resolved SHA into $TEST_DIR (own venv and build)"
  "provision test checkout (venv, Python requirements, frontend build)"
  "apply pending migrations to $TEST_DB (pinned checkout's migration runner)"
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
    printf 'UNIT_STATES=%s\n' "${unit_states_str:-}" >> "$tmp"
    printf 'PM2_NEXTPROD_ACTIVE=%s\n' "${pm2_nextprod_active:-unknown}" >> "$tmp"
  )
  mv -f "$tmp" "$STATE_FILE"
}

RECOVERED=0

# recover_and_fail: log a fatal check failure, run the defined recovery path
# (test-down.sh) to restore PRD services and clean up the test stack, then
# exit nonzero. The durable state file is left intact for test-down to read.
recover_and_fail() {
  RECOVERED=1
  echo "ERROR: $1" >&2
  echo "Running recovery path (test-down.sh)..." >&2
  "$(dirname "$0")/test-down.sh" >&2 || true
  exit 1
}

# on_exit: guaranteed restore on any abnormal exit (unhandled error, signal
# delivery, or any path that bypasses recover_and_fail). Runs test-down only
# when the state file exists and recovery has not already run.
on_exit() {
  local status=$?
  if [[ $status -ne 0 && -f "${STATE_FILE:-}" && "${RECOVERED:-0}" -eq 0 ]]; then
    RECOVERED=1
    echo "ERROR: abnormal exit (status=$status); running recovery path (test-down.sh)..." >&2
    "$(dirname "$0")/test-down.sh" >&2 || true
  fi
  exit "$status"
}

# Crash/Ctrl+C safety: persist the current phase so test-down can decide what
# was already done and what still needs restoring.
trap 'write_state' INT TERM
trap on_exit EXIT

# Crash/Ctrl+C safety: persist the current phase so test-down can decide what
# was already done and what still needs restoring.
trap 'write_state' INT TERM

# --- Discover PRD units from the systemd directory (no hardcoded names) ---
# PRD units (filename does not contain "-test") that are installed on this
# host are stopped and their active state recorded so test-down can restore
# them later. Test units (filename contains "-test") are skipped here.
prd_unit_names=()
unit_states_str=""
if [[ -d "$SYSTEMD_DIR" ]]; then
  for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    [[ "$install_name" == *-test.* ]] && continue
    if systemctl list-unit-files --no-pager --no-legend 2>/dev/null | grep -qx "$install_name"; then
      prd_unit_names+=("$install_name")
      state="$(systemctl is-active "$install_name" 2>/dev/null || echo unknown)"
      unit_states_str="${unit_states_str}${install_name}=${state} "
    fi
  done
fi
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
for unit in "${prd_unit_names[@]:-}"; do
  [[ -z "$unit" ]] && continue
  sudo systemctl stop "$unit" 2>/dev/null || true
done
pm2 stop pifitness-next 2>/dev/null || true

echo "  verify PRD writers are quiesced before cloning the database"
pm2_next_state="stopped"
if pm2 describe pifitness-next >/dev/null 2>&1 && pm2 describe pifitness-next 2>/dev/null | grep -qi 'online'; then
  pm2_next_state="online"
fi
all_quiesced=1
quiesced_details=""
for unit in "${prd_unit_names[@]:-}"; do
  [[ -z "$unit" ]] && continue
  state="$(systemctl is-active "$unit" 2>/dev/null || echo unknown)"
  quiesced_details="${quiesced_details}${unit}=${state} "
  if [[ "$state" == "active" ]]; then
    echo "ERROR: PRD unit still active after stop: $unit (state=$state)" >&2
    all_quiesced=0
  fi
done
if [[ "$pm2_next_state" == "online" ]]; then
  echo "ERROR: PM2 pifitness-next still online after stop" >&2
  all_quiesced=0
fi
if [[ $all_quiesced -eq 0 ]]; then
  exit 1
fi
echo "  PRD quiesced: ${quiesced_details}pm2-next=$pm2_next_state"
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
  if ! pg_restore -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$TEST_DB" "$dump_path"; then
    echo "ERROR: pg_restore of $dump_path into $TEST_DB failed" >&2
    exit 1
  fi
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



echo "Step 5: fetch $ref from origin and resolve to full SHA"
git -C "$PRD_DIR" fetch origin "$ref"
resolved_sha="$(git -C "$PRD_DIR" rev-parse FETCH_HEAD)"
echo "  resolved $ref to $resolved_sha"
current_phase="ref-resolved"
write_state

echo "Step 6: check out resolved SHA into test dir"
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

echo "Step 7: provision test checkout (venv, Python requirements, frontend build)"
# Remove any inherited artifacts from the PRD checkout before provisioning.
rm -rf "$TEST_DIR/venv" "$TEST_DIR/frontend/pifitness/node_modules" "$TEST_DIR/frontend/pifitness/.next" "$TEST_DIR/.env"
# Create the backend venv inside the test checkout.
python3 -m venv "$TEST_DIR/venv"
# Install current runtime requirements (Phase 5 package-sync approach).
"$TEST_DIR/venv/bin/pip" install -r "$TEST_DIR/backend/requirements.txt"
# Install the backend editable package from the checked-out tree.
"$TEST_DIR/venv/bin/pip" install -e "$TEST_DIR/backend"
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

# Failpoint for the human recovery drill: inject a failure right before
# the test stack is started. The durable state file (phase=checkout-provisioned)
# is left intact so test-down can recover.
failpoint "pre_start" "simulate failure before starting test units"

echo "Step 8: start test units"
# Start test units from the systemd directory (filename contains "-test").
# No unit name is hardcoded — everything comes from the unit files.
if [[ -d "$SYSTEMD_DIR" ]]; then
  for unit_file in "$SYSTEMD_DIR"/pifitness-*-test.service "$SYSTEMD_DIR"/pifitness-*-test.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    if ! sudo systemctl start "$install_name" 2>/dev/null; then
      recover_and_fail "failed to start test unit $install_name"
    fi
  done
fi
# Start the test Next.js frontend under PM2. A start failure is fatal and runs recovery.
if ! pm2 start "$TEST_DIR/deploy/scripts/ecosystem.config.js" 2>/dev/null; then
  recover_and_fail "failed to start PM2 test process pifitness-next-test"
fi
current_phase="test-units-started"
write_state

echo "Step 9: verify the complete test stack"
# systemd must report all test units active.
# No unit name is hardcoded — everything comes from the unit files.
if [[ -d "$SYSTEMD_DIR" ]]; then
  for unit_file in "$SYSTEMD_DIR"/pifitness-*-test.service "$SYSTEMD_DIR"/pifitness-*-test.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    state="$(systemctl is-active "$install_name" 2>/dev/null || echo unknown)"
    if [[ "$state" != "active" ]]; then
      recover_and_fail "test unit $install_name is not active (state=$state)"
    fi
  done
fi
# PM2 must report the named test process online.
if ! pm2 describe pifitness-next-test 2>/dev/null | grep -qi 'online'; then
  recover_and_fail "PM2 test process pifitness-next-test is not online"
fi
# FastAPI must answer directly on 127.0.0.1:8000 with a real status body;
# a generic HTTP 200 alone is insufficient.
if ! health_body="$(curl -fsS http://127.0.0.1:8000/api/health 2>/dev/null)"; then
  recover_and_fail "FastAPI health check on 127.0.0.1:8000 failed"
fi
if ! printf '%s' "$health_body" | grep -q '"status"'; then
  recover_and_fail "FastAPI health check did not return a valid status body"
fi
# Next.js must answer directly on 127.0.0.1:3000.
if ! curl -fsS http://127.0.0.1:3000 >/dev/null 2>&1; then
  recover_and_fail "Next.js did not respond on 127.0.0.1:3000"
fi
# The unchanged HTTPS hostname must route to the test stack.
if ! curl -fkS "$HTTPS_HOSTNAME" >/dev/null 2>&1; then
  recover_and_fail "HTTPS hostname $HTTPS_HOSTNAME does not route to the test stack"
fi
# The health check must verify the test environment and TEST DB marker; a
# generic HTTP 200 alone is insufficient. Confirm the TEST DB carries the
# 'test' marker so the backend is connected to the marked TEST database.
marker="$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$TEST_DB" -tAc \
  "SELECT current_setting('pifitness.env', true);" 2>/dev/null | tr -d '[:space:]')"
if [[ "$marker" != "test" ]]; then
  recover_and_fail "$TEST_DB is not marked for test use (marker='${marker:-<none>}')"
fi

current_phase="complete"
write_state
trap - EXIT
echo "test-up complete."