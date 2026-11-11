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
# Full PRD service/timer inventory and suspension are a later, separately
# tested workflow stage; this check is only the existing venv-safety guard.
if systemctl is-active --quiet pifitness-fastapi-test.service; then
    fail "TEST FastAPI service is active. Stop it before reconciling its shared Python environment."
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
    # A failed preflight must never promote a partially prepared checkout.
    # Promotion happens only in the explicit success path below.
    if git -C "$SOURCE_REPO" worktree list --porcelain |
        grep -Fqx "worktree $STAGE_DIR"; then
        git -C "$SOURCE_REPO" worktree remove --force "$STAGE_DIR" ||
            echo "WARNING: failed to remove staging worktree $STAGE_DIR" >&2
    fi
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
echo "Inspecting TEST database against PRD (read-only)..."
mkdir -p "$EVIDENCE_DIR"
chmod 0700 "$EVIDENCE_DIR"
RUN_ID="$(date +%Y%m%d_%H%M%S)_${COMMIT:0:12}_$$"
INSPECTION_REPORT="$EVIDENCE_DIR/test-db-inspection-${RUN_ID}.json"
INSPECTION_ERROR="$EVIDENCE_DIR/test-db-inspection-${RUN_ID}.stderr"
SCHEMA_DIFF_FILE="$EVIDENCE_DIR/test-prd-schema-diff-${RUN_ID}.json"

# The inspector validates both existing environment files, database names,
# actual PostgreSQL identities, PRD's absent marker, and TEST's marker before
# invoking MigraDiff. It performs no DDL and never applies generated SQL.
if ! (
    umask 077
    "$VENV_DIR/bin/python" "$STAGE_DIR/tools/db/inspect_test_db.py" \
        --test-env-file "$ENV_FILE" \
        --prd-env-file "$PRD_ENV_FILE" \
        --allow-prd \
        --diff-file "$SCHEMA_DIFF_FILE" \
        >"$INSPECTION_REPORT" 2>"$INSPECTION_ERROR"
); then
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
    print("  Schema comparison: %s (%s structural differences)" % (
        comparison.get("status", "unknown"), comparison.get("statement_count", "unknown")
    ))
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

case "$DB_ACTION" in
    reuse_test)
        if [[ "$DB_OFFER_REFRESH" == "true" ]]; then
            [[ -t 0 ]] || fail "TEST and PRD are compatible, but the optional PRD freshness choice requires an interactive terminal."
            echo
            read -r -p "TEST schema is compatible with PRD. Refresh TEST from the latest PRD nightly backup for freshness? [y/N]: " refresh_choice
            case "${refresh_choice,,}" in
                y|yes)
                    fail "PRD refresh was requested, but backup restore and rollback are the next integration step. No database or existing checkout was modified."
                    ;;
                n|no|"")
                    echo "Keeping the existing TEST database."
                    ;;
                *)
                    fail "Please answer yes or no. No database or existing checkout was modified."
                    ;;
            esac
        fi
        ;;
    create_from_prd|refresh_from_prd)
        fail "Database policy requires '$DB_ACTION'. Restoring the latest PRD nightly backup and preserving the previous TEST database are the next integration step. No database or existing checkout was modified."
        ;;
    abort)
        fail "Database policy returned abort. See the inspection report above."
        ;;
    *)
        fail "Database inspector returned unknown decision '$DB_ACTION'."
        ;;
esac

echo
echo "All checkout, build, and read-only database preflight checks passed."
echo "TEST database decision: $DB_ACTION"

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
echo "TEST/PRD schema inspection completed read-only."
echo "No database refresh or migrations applied in this integration step."
echo "No services changed."
