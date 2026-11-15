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
HTTPS_HOSTNAME="${HTTPS_HOSTNAME:-https://pifitness.duckdns.org}"
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
      echo "  restore PRD services/timers from $STATE_FILE"
      echo "  call $HEALTH_URL"
      echo "  call $HTTPS_HOSTNAME"
      echo "  remove $TEST_DIR"
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

# --- Validate configuration before any destructive action -----------------
# Canonicalize and validate directory paths (same guard as test-up), then
# recheck DB names and host/port against the recorded state before touching
# anything.
PRD_DIR="$(canonicalize_path "$PRD_DIR")" || exit 1
TEST_DIR="$(canonicalize_path "$TEST_DIR")" || exit 1

if [[ "$PRD_DB" == "$TEST_DB" ]]; then
  echo "ERROR: PRD and TEST DB names must differ" >&2
  exit 1
fi
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

CONFIGURED_DB_HOST="${DB_HOST:-localhost}"
CONFIGURED_DB_PORT="${DB_PORT:-15432}"

# --- Load durable state (D3) -------------------------------------------
# test-up wrote ~/.pifitness-test-state before stopping any PRD process; it
# records what was active (FastAPI, each timer, production PM2 pifitness-next)
# and the phase reached. test-down restores exactly what test-up changed,
# guided by this record, and keeps the state file until PRD health and
# cleanup both succeed so an interrupted run can be retried.
PHASE="none"
if [[ -f "$STATE_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a
  . "$STATE_FILE"
  set +a
fi

# Recheck against the recorded state: fail closed if test-down would connect
# to a different cluster or database pair than the one test-up used.
if [[ "${DB_HOST:-}" != "$CONFIGURED_DB_HOST" || "${DB_PORT:-}" != "$CONFIGURED_DB_PORT" ]]; then
  echo "ERROR: recorded DB host/port $DB_HOST:$DB_PORT does not match configured $CONFIGURED_DB_HOST:$CONFIGURED_DB_PORT" >&2
  exit 1
fi
if [[ "${PRD_DB:-}" == "${TEST_DB:-}" ]]; then
  echo "ERROR: PRD and TEST DB names must differ" >&2
  exit 1
fi

echo "Step 1: stop test units"
# Stop test units from the systemd directory (filename contains "-test").
# No unit name is hardcoded — everything comes from the unit files.
if [[ -d "$SYSTEMD_DIR" ]]; then
  for unit_file in "$SYSTEMD_DIR"/pifitness-*-test.service "$SYSTEMD_DIR"/pifitness-*-test.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    sudo systemctl stop "$install_name" 2>/dev/null || true
  done
fi
pm2 delete pifitness-next-test 2>/dev/null || true

# Failpoint for the human recovery drill: inject a failure during PRD
# restore, before the health check, TEST DB drop, or checkout removal.
# Leaves the state file intact so the drill can be retried.
failpoint "during_restore" "simulate failure during PRD restore (recovery drill)"

echo "Step 2: restore PRD services and timers from the state record"
# UNIT_STATES is a space-separated list of "unit=state" pairs recorded by
# test-up. Restore each PRD unit to the state it was in before test-up.
# No unit name is hardcoded — every unit is discovered from the state file.
if [[ -n "${UNIT_STATES:-}" ]]; then
  for pair in $UNIT_STATES; do
    unit="${pair%%=*}"
    state="${pair#*=}"
    if [[ "$state" == "active" ]]; then
      if ! sudo systemctl start "$unit"; then
        echo "ERROR: failed to restart $unit (was active before test-up)" >&2
        exit 1
      fi
    else
      echo "  $unit was not active before test-up; leaving stopped"
    fi
  done
fi
if [[ "${PM2_NEXTPROD_ACTIVE:-unknown}" == "online" ]]; then
  if ! pm2 restart pifitness-next; then
    echo "ERROR: failed to restart production PM2 pifitness-next (was online before test-up)" >&2
    exit 1
  fi
  echo "  production PM2 pifitness-next was online before test-up; restarted"
else
  echo "  production PM2 pifitness-next was not online before test-up; leaving stopped"
fi

echo "Step 3: health check (PRD API must respond before cleanup)"
# curl is required: a missing curl or a failed health check aborts here,
# before the TEST database or checkout is touched, and the state file is
# retained so the run can be retried.
if ! command -v curl >/dev/null 2>&1; then
  echo "ERROR: curl is required for the PRD health check" >&2
  exit 1
fi
if ! curl -fsS "$HEALTH_URL" >/dev/null; then
  echo "ERROR: PRD health check failed at $HEALTH_URL; state file retained for retry" >&2
  exit 1
fi
# The unchanged public HTTPS hostname must route to the restored PRD API.
if ! curl -fkS "$HTTPS_HOSTNAME" >/dev/null 2>&1; then
  echo "ERROR: PRD health check failed at HTTPS hostname $HTTPS_HOSTNAME; state file retained for retry" >&2
  exit 1
fi

# The PRD dump preserved by test-up is a disaster-recovery artifact only:
# test-down never restores PRD from it, so PRD data remains untouched.
echo "Step 4: drop test DB (marker check before any drop)"
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

echo "Step 5: remove test checkout"
# Harden checkout removal with the same canonical-path guard as test-up:
# refuse TEST_DIR equal to or an ancestor of PRD_DIR before any rm -rf.
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
rm -rf "$TEST_DIR"

echo "Step 6: remove state file (cleanup complete)"
rm -f "$STATE_FILE"

echo "test-down complete."