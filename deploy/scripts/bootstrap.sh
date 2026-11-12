#!/usr/bin/env bash
set -Eeuo pipefail
cd /

usage() {
    echo "Usage: bootstrap.sh --environment prd|test --commit SHA"
}

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

ENVIRONMENT=""
COMMIT=""

while (($#)); do
    case "$1" in
        --environment)
            [[ $# -ge 2 ]] || { usage; exit 64; }
            ENVIRONMENT="${2,,}"
            shift 2
            ;;
        --commit)
            [[ $# -ge 2 ]] || { usage; exit 64; }
            COMMIT="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 64
            ;;
    esac
done

case "$ENVIRONMENT" in
    test)
        BRANCH="feature-test"
        APP_DIR="/home/god/PiFitness-test"
        ENV_FILE="/home/god/.config/pifitness/test.env"
        VENV_DIR="/home/god/.local/share/pifitness/test-venv"
        ;;
    prd)
        echo "PRD deployment is not yet implemented."
        exit 2
        ;;
    *)
        echo "ERROR: Environment must be prd or test." >&2
        usage
        exit 64
        ;;
esac

SOURCE_REPO="/home/god/PiFitness"
PRD_ENV_FILE="$SOURCE_REPO/backend/.env"
UV_BIN="/home/god/.local/bin/uv"
PYTHON_BIN="/usr/bin/python3"
NODE_BIN="/usr/bin/node"
NPM_BIN="/usr/bin/npm"
EVIDENCE_DIR="/home/god/Documents/PiFitness_Test_Evidence"
JOURNAL_DIR="$EVIDENCE_DIR/journals"
# Historical PiFitness backup location/name. Override these two values if the
# nightly backup job uses a different directory or custom-archive filename.
PRD_BACKUP_DIR="${PIFITNESS_PRD_BACKUP_DIR:-$HOME/Documents/DB_Backups}"
PRD_BACKUP_GLOB="${PIFITNESS_PRD_BACKUP_GLOB:-personal_fitness_[0-9]*.dump}"
DB_RECOVERY_STATE_FILE=""
DB_RECOVERY_SCRIPT=""

if [[ ! "$COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]; then
    fail "A full 40-character commit SHA is required."
fi

[[ -f "$ENV_FILE" ]] || fail "Missing TEST environment file: $ENV_FILE"
[[ -f "$PRD_ENV_FILE" ]] || fail "Missing PRD environment file: $PRD_ENV_FILE"
[[ -x "$UV_BIN" ]] || fail "uv not found or not executable: $UV_BIN"
[[ -x "$PYTHON_BIN" ]] || fail "Python not found: $PYTHON_BIN"
[[ -x "$NODE_BIN" ]] || fail "Node.js not found: $NODE_BIN"
[[ -x "$NPM_BIN" ]] || fail "npm not found: $NPM_BIN"

# Do not reconcile the persistent TEST environment while its API is running.
# Check all TEST units from the systemd directory — no unit name is hardcoded.
# Test units are those whose filename contains "-test".
TEST_SYSTEMD_DIR="${APP_DIR}/deploy/systemd"
if [[ -d "$TEST_SYSTEMD_DIR" ]]; then
  for unit_file in "$TEST_SYSTEMD_DIR"/pifitness-*-test.service "$TEST_SYSTEMD_DIR"/pifitness-*-test.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    if systemctl is-active --quiet "$install_name" 2>/dev/null; then
      fail "TEST unit $install_name is active. Stop it before reconciling its shared Python environment."
    fi
  done
fi

if [[ -L "$VENV_DIR" ]]; then
    fail "Persistent TEST virtual environment must not itself be a symlink: $VENV_DIR"
fi

if [[ -e "$VENV_DIR" && ! -d "$VENV_DIR" ]]; then
    fail "Persistent TEST virtual environment path is not a directory: $VENV_DIR"
fi

mkdir -p "$(dirname "$VENV_DIR")"

git -C "$SOURCE_REPO" cat-file -e "${COMMIT}^{commit}"

git -C "$SOURCE_REPO" merge-base --is-ancestor \
    "$COMMIT" "origin/$BRANCH" || {
        fail "Commit is not part of origin/$BRANCH."
    }

if [[ -e "$APP_DIR" || -L "$APP_DIR" ]]; then
    git -C "$SOURCE_REPO" worktree list --porcelain |
        grep -Fqx "worktree $APP_DIR" || {
            fail "Existing TEST path is not a registered worktree."
        }
fi

STAGE_DIR="${APP_DIR}.staging.$$"

if [[ -e "$STAGE_DIR" || -L "$STAGE_DIR" ]]; then
    fail "Staging path already exists: $STAGE_DIR"
fi

cleanup() {
    local exit_status=$?
    trap - EXIT

    # Once the PRD-derived TEST database has passed restore, ownership,
    # marker, permission, and schema verification, it is the new baseline.
    # A later checkout/build/migration failure must NOT restore the known-bad
    # previous TEST database. Keep the clean baseline for the next attempt.
    if [[ "$exit_status" -ne 0 && -n "$DB_RECOVERY_STATE_FILE" \
          && -f "$DB_RECOVERY_STATE_FILE" ]]; then
        recovery_status=$("$VENV_DIR/bin/python" - "$DB_RECOVERY_STATE_FILE" <<'PY_RECOVERY_STATUS' 2>/dev/null || true
import json
import sys
try:
    with open(sys.argv[1], encoding="utf-8") as stream:
        print(json.load(stream).get("status", "unknown"))
except Exception:
    print("unknown")
PY_RECOVERY_STATUS
)
        if [[ "$recovery_status" == "baseline_ready" ]]; then
            echo "Bootstrap stopped after TEST database recovery. The verified PRD-derived TEST baseline is retained for the next attempt." >&2
        else
            echo "Bootstrap stopped during TEST database recovery (journal status: ${recovery_status:-unknown}). No verified baseline is being claimed." >&2
        fi
        echo "Recovery journal: $DB_RECOVERY_STATE_FILE" >&2
    fi

    # A failed preflight must never promote a partially prepared checkout.
    # Promotion happens only in the explicit success path below.
    if git -C "$SOURCE_REPO" worktree list --porcelain |
        grep -Fqx "worktree $STAGE_DIR"; then
        git -C "$SOURCE_REPO" worktree remove --force "$STAGE_DIR" ||
            echo "WARNING: failed to remove staging worktree $STAGE_DIR" >&2
    fi
    exit "$exit_status"
}
trap cleanup EXIT

echo "Preparing clean TEST checkout..."
git -C "$SOURCE_REPO" worktree add --detach "$STAGE_DIR" "$COMMIT"

STAGE_ENV_LINK="$STAGE_DIR/backend/.env"
ln -s "$ENV_FILE" "$STAGE_ENV_LINK"

# The virtual environment lives outside the replaceable Git checkout.
# Its symlink is recreated in every new checkout.
ln -s "$VENV_DIR" "$STAGE_DIR/venv"

ACTUAL_COMMIT=$(git -C "$STAGE_DIR" rev-parse HEAD)

[[ "$ACTUAL_COMMIT" == "$COMMIT" ]] ||
    fail "Staged commit verification failed."

[[ -f "$STAGE_DIR/backend/pyproject.toml" ]] ||
    fail "Python project configuration is missing."

[[ -f "$STAGE_DIR/backend/uv.lock" ]] ||
    fail "Committed Python lockfile is missing."

echo
echo "Synchronizing TEST Python dependencies..."
(
    cd "$STAGE_DIR/backend"

    UV_PROJECT_ENVIRONMENT="$VENV_DIR" \
        "$UV_BIN" sync \
            --locked \
            --no-dev \
            --group deployment \
            --no-editable \
            --python "$PYTHON_BIN"
)

# The deployment dependency group is part of backend/pyproject.toml and
# backend/uv.lock. Keeping MigraDiff inside this sync prevents uv from
# removing it on the next run and avoids a second package-install operation.
# Confirm the exact comparator version before any database inspection.
MIGRADIFF_VERSION=$("$VENV_DIR/bin/python" -c \
    'import importlib.metadata; print(importlib.metadata.version("migradiff"))')
[[ "$MIGRADIFF_VERSION" == "1.7.2" ]] ||
    fail "Expected MigraDiff 1.7.2, found $MIGRADIFF_VERSION."

echo
echo "Reconciling any interrupted TEST database recovery..."
mkdir -p "$EVIDENCE_DIR"
chmod 0700 "$EVIDENCE_DIR"
mkdir -p "$JOURNAL_DIR"
chmod 0700 "$JOURNAL_DIR"
DB_RECOVERY_SCRIPT="$STAGE_DIR/tools/db/restore_test_db.py"
[[ -f "$DB_RECOVERY_SCRIPT" ]] || fail "TEST database recovery helper is missing: $DB_RECOVERY_SCRIPT"
PENDING_RECOVERY_REPORT="$EVIDENCE_DIR/test-db-recovery-reconcile-$(date +%Y%m%d_%H%M%S)_$$.json"
PENDING_RECOVERY_ERROR="${PENDING_RECOVERY_REPORT%.json}.stderr"
if ! (
    umask 077
    "$VENV_DIR/bin/python" "$DB_RECOVERY_SCRIPT" \
        --test-env-file "$ENV_FILE" --prd-env-file "$PRD_ENV_FILE" \
        recover-pending --state-dir "$JOURNAL_DIR"
) >"$PENDING_RECOVERY_REPORT" 2>"$PENDING_RECOVERY_ERROR"; then
    [[ ! -s "$PENDING_RECOVERY_ERROR" ]] || cat "$PENDING_RECOVERY_ERROR" >&2
    echo "Recovery reconciliation report: $PENDING_RECOVERY_REPORT" >&2
    fail "An interrupted TEST database recovery could not be reconciled. Manual database inspection is required."
fi
chmod 0600 "$PENDING_RECOVERY_REPORT"
[[ ! -s "$PENDING_RECOVERY_ERROR" ]] || chmod 0600 "$PENDING_RECOVERY_ERROR"
"$VENV_DIR/bin/python" - "$PENDING_RECOVERY_REPORT" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as stream:
    report = json.load(stream)
for path in report.get("cleaned", []):
    print("  Removed an incomplete TEST restore so the next attempt can start cleanly: %s" % path)
for path in report.get("baseline_ready", []):
    print("  Retained verified PRD-derived TEST baseline (no re-restore needed): %s" % path)
PY

echo
echo "Inspecting TEST database against PRD (read-only)..."
RUN_ID="$(date +%Y%m%d_%H%M%S)_${COMMIT:0:12}_$$"
INSPECTION_REPORT="$EVIDENCE_DIR/test-db-inspection-${RUN_ID}.json"
INSPECTION_ERROR="$EVIDENCE_DIR/test-db-inspection-${RUN_ID}.stderr"
SCHEMA_DIFF_FILE="$EVIDENCE_DIR/test-prd-schema-diff-${RUN_ID}.json"
if ! (
    umask 077
    "$VENV_DIR/bin/python" "$STAGE_DIR/tools/db/inspect_test_db.py" \
        --test-env-file "$ENV_FILE" \
        --prd-env-file "$PRD_ENV_FILE" \
        --allow-prd \
        --diff-file "$SCHEMA_DIFF_FILE" \
        --baseline-state-dir "$JOURNAL_DIR"
) >"$INSPECTION_REPORT" 2>"$INSPECTION_ERROR"; then
    [[ ! -s "$INSPECTION_ERROR" ]] || cat "$INSPECTION_ERROR" >&2
    echo "Structured inspection report: $INSPECTION_REPORT" >&2
    echo "No database or existing checkout was modified by this inspection." >&2
    exit 1
fi
chmod 0600 "$INSPECTION_REPORT"
[[ ! -s "$INSPECTION_ERROR" ]] || chmod 0600 "$INSPECTION_ERROR"
echo "Database inspection report: $INSPECTION_REPORT"
[[ ! -f "$SCHEMA_DIFF_FILE" ]] || echo "Schema comparison artifact: $SCHEMA_DIFF_FILE"
"$VENV_DIR/bin/python" - "$INSPECTION_REPORT" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as stream:
    report = json.load(stream)
test = report.get("test") or {}
prd = report.get("prd") or {}
comparison = report.get("schema_comparison") or {}
decision = report.get("decision") or {}
if prd.get("database"):
    marker = "unmarked" if not prd.get("marker") else repr(prd["marker"])
    print("  PRD: %s (marker %s)" % (prd["database"], marker))
if test.get("database"):
    if test.get("exists") is False:
        print("  TEST: %s (does not exist)" % test["database"])
    elif test.get("exists") is True:
        print("  TEST: %s (marker %s)" % (test["database"], repr(test.get("marker"))))
if comparison:
    if comparison.get("status") == "migration_managed":
        provenance = report.get("baseline_provenance") or {}
        migration_status = provenance.get("migration_status") or {}
        print("  Schema state: migration-managed (%s applied, %s pending; checksums verified)" % (
            migration_status.get("applied", "unknown"), migration_status.get("pending", "unknown")))
    else:
        print("  Schema comparison: %s (%s structural differences)" % (
            comparison.get("status", "unknown"), comparison.get("statement_count", "unknown")))
print("  Database decision: %s" % decision.get("action", "unknown"))
if decision.get("reason"):
    print("  Reason: %s" % decision["reason"])
PY

DB_DECISION_FIELDS=$("$VENV_DIR/bin/python" - "$INSPECTION_REPORT" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as stream:
    report = json.load(stream)
decision = report.get("decision") or {}
action = decision.get("action", "abort")
offer_refresh = "true" if decision.get("offer_refresh") is True else "false"
print(action + "|" + offer_refresh)
PY
)
IFS='|' read -r DB_ACTION DB_OFFER_REFRESH <<< "$DB_DECISION_FIELDS"

# Enumerate available PRD nightly backups and compute the age of the latest.
LATEST_BACKUP=""
LATEST_BACKUP_AGE_HOURS=""
LATEST_BACKUP_EPOCH=""
BACKUP_COUNT=0
shopt -s nullglob
for backup_file in "$PRD_BACKUP_DIR"/$PRD_BACKUP_GLOB; do
    BACKUP_COUNT=$((BACKUP_COUNT + 1))
    if [[ "$backup_file" =~ _([0-9]{8}_[0-9]{6})\.dump$ ]]; then
        BACKUP_TS="${BASH_REMATCH[1]}"
        BACKUP_DATETIME="${BACKUP_TS:0:4}-${BACKUP_TS:4:2}-${BACKUP_TS:6:2} ${BACKUP_TS:9:2}:${BACKUP_TS:11:2}:${BACKUP_TS:13:2}"
        BACKUP_EPOCH="$(date -d "$BACKUP_DATETIME" +%s 2>/dev/null || true)"
        if [[ -n "$BACKUP_EPOCH" ]]; then
            NOW_EPOCH="$(date +%s)"
            AGE_SECONDS=$((NOW_EPOCH - BACKUP_EPOCH))
            AGE_HOURS=$((AGE_SECONDS / 3600))
            if [[ -z "$LATEST_BACKUP_EPOCH" || "$BACKUP_EPOCH" -gt "$LATEST_BACKUP_EPOCH" ]]; then
                LATEST_BACKUP_EPOCH="$BACKUP_EPOCH"
                LATEST_BACKUP="$backup_file"
                LATEST_BACKUP_AGE_HOURS="$AGE_HOURS"
            fi
        fi
    fi
done
shopt -u nullglob

NEEDS_DB_RECOVERY=0
DB_RECOVERY_SOURCE=""
FRESH_DUMP=""

case "$DB_ACTION" in
    abort)
        fail "Database inspector returned abort. See the inspection report above."
        ;;
    reuse_test)
        if [[ "$DB_OFFER_REFRESH" == "true" && $BACKUP_COUNT -gt 0 ]]; then
            echo
            echo "Choose an option:"
            echo "  1) Keep existing TEST database (unchanged)"
            echo "  2) Refresh from latest PRD nightly backup"
            if [[ -n "$LATEST_BACKUP" ]]; then
                echo "      Latest backup: $(basename "$LATEST_BACKUP") (age: ${LATEST_BACKUP_AGE_HOURS} hour(s))"
            fi
            echo "  3) Fresh dump of live PRD (pg_dump then restore)"
            [[ -t 0 ]] || fail "User input required for TEST database choice, but no interactive terminal is available."
            read -r -p "Enter choice [1-3]: " db_choice
            case "$db_choice" in
                1)
                    echo "Keeping the existing TEST database."
                    ;;
                2)
                    NEEDS_DB_RECOVERY=1
                    DB_RECOVERY_SOURCE="nightly"
                    ;;
                3)
                    NEEDS_DB_RECOVERY=1
                    DB_RECOVERY_SOURCE="fresh_dump"
                    ;;
                *)
                    fail "Invalid choice '$db_choice'. No database was modified."
                    ;;
            esac
        elif [[ $BACKUP_COUNT -eq 0 ]]; then
            echo
            echo "No PRD nightly backups found in $PRD_BACKUP_DIR. Keeping existing TEST database."
        fi
        ;;
    create_from_prd|refresh_from_prd)
        if [[ $BACKUP_COUNT -eq 0 ]]; then
            fail "Database inspector requests a PRD refresh, but no nightly backups were found in $PRD_BACKUP_DIR."
        fi
        NEEDS_DB_RECOVERY=1
        DB_RECOVERY_SOURCE="nightly"
        ;;
    *)
        fail "Database inspector returned unknown decision '$DB_ACTION'."
        ;;
esac

if [[ "$NEEDS_DB_RECOVERY" == "1" ]]; then
    DB_RECOVERY_STATE_FILE="$JOURNAL_DIR/test-db-recovery-${RUN_ID}.json"
    RECOVERY_REPORT="$EVIDENCE_DIR/test-db-recovery-report-${RUN_ID}.json"
    RECOVERY_ERROR="$EVIDENCE_DIR/test-db-recovery-${RUN_ID}.stderr"

    if [[ "$DB_RECOVERY_SOURCE" == "fresh_dump" ]]; then
        echo
        echo "Taking a fresh dump of live PRD..."
        set -a
        . "$PRD_ENV_FILE"
        set +a
        FRESH_DUMP="$PRD_BACKUP_DIR/personal_fitness_$(date +%Y%m%d_%H%M%S).dump"
        if ! PGPASSWORD="${PGPASSWORD:-}" pg_dump -F c -h "${PG_HOST:-localhost}" -p "${PG_PORT:-5432}" -U "${PG_USER:-postgres}" -d "${PG_DB:-pifitness}" >"$FRESH_DUMP"; then
            fail "Fresh PRD dump failed. No database was modified."
        fi
        echo "Fresh PRD dump written to: $FRESH_DUMP"
        DB_RECOVERY_ARGS=(--backup-file "$FRESH_DUMP")
    else
        echo
        echo "Restoring TEST database from the latest PRD nightly backup..."
        DB_RECOVERY_ARGS=(--backup-dir "$PRD_BACKUP_DIR" --backup-glob "$PRD_BACKUP_GLOB" --max-backup-age-hours 24)
    fi

    if ! (
        umask 077
        "$VENV_DIR/bin/python" "$DB_RECOVERY_SCRIPT" \
            --test-env-file "$ENV_FILE" --prd-env-file "$PRD_ENV_FILE" \
            restore \
            "${DB_RECOVERY_ARGS[@]}" \
            --state-file "$DB_RECOVERY_STATE_FILE" \
            --diff-file "$SCHEMA_DIFF_FILE"
    ) >"$RECOVERY_REPORT" 2>"$RECOVERY_ERROR"; then
        [[ ! -s "$RECOVERY_ERROR" ]] || cat "$RECOVERY_ERROR" >&2
        echo "Database recovery evidence: $RECOVERY_REPORT" >&2
        [[ ! -f "$DB_RECOVERY_STATE_FILE" ]] || echo "Database recovery journal: $DB_RECOVERY_STATE_FILE" >&2
        fail "TEST database recovery failed. Any incomplete replacement was removed; inspect the recovery journal before retrying."
    fi
    chmod 0600 "$RECOVERY_REPORT" "$DB_RECOVERY_STATE_FILE"
    [[ ! -s "$RECOVERY_ERROR" ]] || chmod 0600 "$RECOVERY_ERROR"
    echo "Database recovery report: $RECOVERY_REPORT"
    echo "Database recovery journal: $DB_RECOVERY_STATE_FILE"

    "$VENV_DIR/bin/python" - "$RECOVERY_REPORT" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as stream:
    report = json.load(stream)
if not report.get("ok"):
    raise SystemExit("Recovery report did not confirm success.")
print("  Restored TEST database: %s" % report.get("test_database"))
print("  Source PRD backup: %s" % report.get("backup_path"))
print("  Marker: %s" % report.get("marker"))
print("  Previous TEST database discarded: %s" % report.get("previous_test_database_discarded"))
comparison = report.get("schema_comparison") or {}
print("  Live PRD comparison (diagnostic only): %s" % comparison.get("status", "unavailable"))
if comparison.get("status") == "different":
    print("  Note: the nightly archive may predate later live-PRD schema changes; the restored baseline is retained.")
PY

    POST_INSPECTION_REPORT="$EVIDENCE_DIR/test-db-inspection-after-recovery-${RUN_ID}.json"
    POST_INSPECTION_ERROR="$EVIDENCE_DIR/test-db-inspection-after-recovery-${RUN_ID}.stderr"
    echo
    echo "Verifying restored TEST database identity, permissions, and baseline provenance..."
    if ! (
        umask 077
        "$VENV_DIR/bin/python" "$STAGE_DIR/tools/db/inspect_test_db.py" \
            --test-env-file "$ENV_FILE" \
            --prd-env-file "$PRD_ENV_FILE" \
            --allow-prd \
            --diff-file "$SCHEMA_DIFF_FILE" \
            --baseline-state-dir "$JOURNAL_DIR"
    ) >"$POST_INSPECTION_REPORT" 2>"$POST_INSPECTION_ERROR"; then
        [[ ! -s "$POST_INSPECTION_ERROR" ]] || cat "$POST_INSPECTION_ERROR" >&2
        echo "Post-recovery inspection report: $POST_INSPECTION_REPORT" >&2
        fail "Restored TEST database did not pass the independent post-restore inspection. The verified baseline is retained for diagnosis; no previous TEST database will be restored."
    fi
    chmod 0600 "$POST_INSPECTION_REPORT"
    [[ ! -s "$POST_INSPECTION_ERROR" ]] || chmod 0600 "$POST_INSPECTION_ERROR"
    "$VENV_DIR/bin/python" - "$POST_INSPECTION_REPORT" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as stream:
    report = json.load(stream)
test = report.get("test") or {}
comparison = report.get("schema_comparison") or {}
decision = report.get("decision") or {}
if not report.get("ok"):
    raise SystemExit("Post-recovery inspection did not succeed.")
if test.get("marker") != "test":
    raise SystemExit("Post-recovery TEST marker is not exactly 'test'.")
if comparison.get("status") not in {"identical", "restored_baseline", "migration_managed"}:
    raise SystemExit("Restored TEST baseline was not accepted by the inspector: %r" % comparison.get("status"))
if decision.get("action") != "reuse_test":
    raise SystemExit("Unexpected post-recovery database decision: %r" % decision.get("action"))
print("  TEST database: %s (marker test)" % test.get("database"))
print("  TEST baseline state: %s" % comparison.get("status"))
provenance = report.get("baseline_provenance") or {}
migration_status = provenance.get("migration_status") or {}
if migration_status:
    print("  Migration ledger: %s applied, %s pending; safe to resume" % (
        migration_status.get("applied", 0), migration_status.get("pending", 0)))
PY
    INSPECTION_REPORT="$POST_INSPECTION_REPORT"
fi

echo

echo "Verifying application imports..."
(
    cd "$STAGE_DIR"

    "$VENV_DIR/bin/python" - <<'PY'
from backend.main import app

print("Application import successful")
print("Title:", app.title)
PY
)

echo
echo "Ensuring matching Playwright Chromium is installed..."
"$VENV_DIR/bin/python" -m playwright install chromium

echo
echo "Verifying Playwright browser launch..."
(
    cd "$STAGE_DIR"

    "$VENV_DIR/bin/python" - <<'PY'
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print("Chromium launch successful:", browser.version)
    browser.close()
PY
)

echo
echo "Installing TEST frontend dependencies and building..."

FRONTEND_DIR="$STAGE_DIR/frontend/pifitness"

[[ -f "$FRONTEND_DIR/package.json" ]] ||
    fail "Frontend package.json is missing."

[[ -f "$FRONTEND_DIR/package-lock.json" ]] ||
    fail "Frontend package-lock.json is missing."

(
    cd "$FRONTEND_DIR"
    "$NPM_BIN" ci
    "$NPM_BIN" run build
)

echo
echo "Frontend dependencies installed and production build verified."

echo
echo
echo "All checkout, build, and read-only database preflight checks passed."
echo "TEST database decision: $DB_ACTION"
if [[ -n "$DB_RECOVERY_STATE_FILE" ]]; then
    echo "TEST database recovery journal (verified baseline provenance): $DB_RECOVERY_STATE_FILE"
fi

PREVIOUS_DIR="${APP_DIR}.previous.$$"
if [[ -e "$APP_DIR" || -L "$APP_DIR" ]]; then
    [[ ! -e "$PREVIOUS_DIR" && ! -L "$PREVIOUS_DIR" ]] ||
        fail "Previous-checkout preservation path already exists: $PREVIOUS_DIR"

    echo "Temporarily preserving the current TEST checkout..."
    git -C "$SOURCE_REPO" worktree move "$APP_DIR" "$PREVIOUS_DIR"

    if ! git -C "$SOURCE_REPO" worktree move "$STAGE_DIR" "$APP_DIR"; then
        echo "ERROR: failed to promote the staged TEST checkout; restoring the previous checkout..." >&2
        if git -C "$SOURCE_REPO" worktree move "$PREVIOUS_DIR" "$APP_DIR"; then
            fail "Staged checkout promotion failed. Previous TEST checkout was restored."
        else
            fail "Staged checkout promotion and automatic checkout restoration both failed. Previous checkout remains at $PREVIOUS_DIR; manual recovery is required."
        fi
    fi

    # This bootstrap increment ends after the preflight/checkout stage. Later
    # acceptance/rollback integration will retain this worktree for the full
    # TEST window instead of removing it here.
    if ! git -C "$SOURCE_REPO" worktree remove --force "$PREVIOUS_DIR"; then
        echo "WARNING: new TEST checkout is active, but previous checkout cleanup failed: $PREVIOUS_DIR" >&2
    fi
else
    git -C "$SOURCE_REPO" worktree move "$STAGE_DIR" "$APP_DIR"
fi

trap - EXIT

echo
echo "TEST checkout successfully replaced."
echo "Path:   $APP_DIR"
echo "Commit: $(git -C "$APP_DIR" rev-parse HEAD)"

echo
echo "Persistent virtual environment:"
readlink -f "$APP_DIR/venv"

echo
echo "Environment link:"
ls -l "$APP_DIR/backend/.env"

echo
echo "Dependencies synchronized from committed uv.lock."
echo "MigraDiff 1.7.2 verified."
echo "TEST/PRD schema inspection completed."
if [[ -n "$DB_RECOVERY_STATE_FILE" ]]; then
    echo "TEST database restored from the latest validated PRD backup and marked test."
    echo "Previous TEST database discarded after successful replacement verification."
    echo "The PRD-derived TEST baseline remains in place if a later bootstrap step fails."
else
    echo "Existing TEST database reused; no database refresh was applied."
fi
echo "No migrations applied in this integration step."
echo "No services changed."
