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
UV_BIN="/home/god/.local/bin/uv"
PYTHON_BIN="/usr/bin/python3"
NODE_BIN="/usr/bin/node"
NPM_BIN="/usr/bin/npm"

if [[ ! "$COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]; then
    fail "A full 40-character commit SHA is required."
fi

[[ -f "$ENV_FILE" ]] || fail "Missing environment file: $ENV_FILE"
[[ -x "$UV_BIN" ]] || fail "uv not found or not executable: $UV_BIN"
[[ -x "$PYTHON_BIN" ]] || fail "Python not found: $PYTHON_BIN"
[[ -x "$NODE_BIN" ]] || fail "Node.js not found: $NODE_BIN"
[[ -x "$NPM_BIN" ]] || fail "npm not found: $NPM_BIN"

command -v pg_dump >/dev/null 2>&1 ||
    fail "Required PostgreSQL executable pg_dump is missing."

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
    if git -C "$SOURCE_REPO" worktree list --porcelain |
        grep -Fqx "worktree $STAGE_DIR"; then

        if [[ ! -e "$APP_DIR" && ! -L "$APP_DIR" ]]; then
            git -C "$SOURCE_REPO" worktree move \
                "$STAGE_DIR" "$APP_DIR" ||
                git -C "$SOURCE_REPO" worktree remove --force "$STAGE_DIR"
        else
            git -C "$SOURCE_REPO" worktree remove --force "$STAGE_DIR"
        fi
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
            --no-editable \
            --python "$PYTHON_BIN"
)

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
echo "All pre-deployment checks passed."

if [[ -e "$APP_DIR" || -L "$APP_DIR" ]]; then
    echo "Replacing previous TEST checkout..."
    git -C "$SOURCE_REPO" worktree remove --force "$APP_DIR"
fi

git -C "$SOURCE_REPO" worktree move "$STAGE_DIR" "$APP_DIR"

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
echo "Application import verified."
echo "Playwright Chromium verified."
echo "No migrations applied."
echo "No services changed."
