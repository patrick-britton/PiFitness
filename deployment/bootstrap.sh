#!/bin/bash
# PiFitness Bootstrap Launcher
# Usage: bash bootstrap.sh [options]
#
# Modes (interactive or via flags):
#   1) Full deploy   --install-packages    (or run with no flags + menu option 1)
#   2) Fast deploy   --fast
#   3) Nuclear       --nuclear
#   4) Pull only     --pull                (git pull react-ui, no deployment)
#
# Flow:
#   1. Validates environment
#   2. (Modes 1-3) Fetches deployment scripts from origin/deployment_script
#      and runs deploy_react.sh
#   3. (Mode 4) Pulls the react-ui branch and exits

set -e

# --- Constants ---
PROJECT_DIR="/home/god/PiFitness"
TARGET="react-ui"   # Only one deployment target now (Streamlit is retired)

# --- Colours ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# --- Helpers ---
error_exit() {
    echo -e "${RED}ERROR: $1${NC}" >&2
    exit 1
}

info() {
    echo -e "${GREEN}INFO: $1${NC}"
}

warn() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

# --- Parse CLI args ---
INSTALL_PACKAGES=""
FAST=""
NUCLEAR=""
PULL_ONLY=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --install-packages)
            INSTALL_PACKAGES=true
            shift
            ;;
        --fast)
            FAST=true
            NUCLEAR=false
            PULL_ONLY=false
            shift
            ;;
        --nuclear)
            NUCLEAR=true
            FAST=false
            PULL_ONLY=false
            shift
            ;;
        --pull)
            PULL_ONLY=true
            FAST=false
            NUCLEAR=false
            shift
            ;;
        *)
            error_exit "Unknown argument: $1"
            ;;
    esac
done

# --- Interactive prompts if no mode was provided on the CLI ---
if [[ -z "$FAST" && -z "$NUCLEAR" && -z "$PULL_ONLY" ]]; then
    echo ""
    echo "Select deployment mode:"
    echo "  1) Full deploy (pull code, tests, install packages, rebuild, restart services)"
    echo "  2) Fast deploy (pull code, skip tests/packages, restart services only)"
    echo "  3) Nuclear (wipe everything, re-clone, rebuild from scratch)"
    echo "  4) Pull only (git pull react-ui branch, no deployment)"
    read -rp "Enter 1, 2, 3, or 4: " mode_choice

    case $mode_choice in
        1) FAST=false; NUCLEAR=false; PULL_ONLY=false ;;
        2) FAST=true;  NUCLEAR=false; PULL_ONLY=false ;;
        3) FAST=false; NUCLEAR=true;  PULL_ONLY=false ;;
        4) FAST=false; NUCLEAR=false; PULL_ONLY=true  ;;
        *) error_exit "Invalid choice. Exiting." ;;
    esac

    # Only ask about packages when doing a full deploy
    if [[ "$FAST" != "true" && "$NUCLEAR" != "true" && "$PULL_ONLY" != "true" ]]; then
        echo ""
        echo "Install/verify packages?"
        echo "  1) Yes, install/update Python and npm dependencies"
        echo "  2) No, skip package checks"
        read -rp "Enter 1 or 2: " pkg_choice
        case $pkg_choice in
            1) INSTALL_PACKAGES=true ;;
            2) INSTALL_PACKAGES=false ;;
            *) error_exit "Invalid choice. Exiting." ;;
        esac
    fi
fi

# Defaults for any unset values
INSTALL_PACKAGES="${INSTALL_PACKAGES:-false}"
FAST="${FAST:-false}"
NUCLEAR="${NUCLEAR:-false}"
PULL_ONLY="${PULL_ONLY:-false}"

# Sanity checks
if [[ "$FAST" == true && "$NUCLEAR" == true ]]; then
    error_exit "--fast and --nuclear are mutually exclusive"
fi
if [[ "$PULL_ONLY" == true && ("$FAST" == true || "$NUCLEAR" == true) ]]; then
    error_exit "--pull cannot be combined with --fast or --nuclear"
fi

info "Target: $TARGET"
info "Options: install-packages=$INSTALL_PACKAGES fast=$FAST nuclear=$NUCLEAR pull=$PULL_ONLY"

# --- Pre-flight checks ---
info "Running pre-flight checks..."

# Disk space (at least 2 GB free)
AVAILABLE_SPACE=$(df /home/god --output=avail 2>/dev/null | tail -1)
if [[ -z "$AVAILABLE_SPACE" ]]; then
    warn "Could not check disk space. Continuing anyway."
elif [[ "$AVAILABLE_SPACE" -lt 2097152 ]]; then
    error_exit "Insufficient disk space: ${AVAILABLE_SPACE}KB available, need at least 2GB"
fi

# Master .env exists
if [[ ! -f "/home/god/Documents/.env" ]]; then
    warn "Master .env not found at /home/god/Documents/.env"
    warn "Deployment will continue, but services may fail without environment variables"
fi

# Git available
if ! command -v git &> /dev/null; then
    error_exit "Git is not installed"
fi

# Venv exists (skip check for nuclear and pull-only)
VENV_DIR="$PROJECT_DIR/venv"
if [[ ! -d "$VENV_DIR" && "$NUCLEAR" == false && "$PULL_ONLY" == false ]]; then
    warn "Virtual environment not found at $VENV_DIR"
    warn "Will create it during deployment"
fi

# Target branch exists locally
if git -C "$PROJECT_DIR" show-ref --verify --quiet "refs/heads/$TARGET" 2>/dev/null; then
    info "Target branch '$TARGET' exists locally"
else
    warn "Target branch '$TARGET' not found locally. Will fetch from origin."
fi

info "Pre-flight checks passed."

# --- Mode 4: Pull only ---
if [[ "$PULL_ONLY" == "true" ]]; then
    info "=== PULL ONLY MODE ==="
    cd "$PROJECT_DIR"

    info "Fetching origin/$TARGET..."
    git fetch origin "$TARGET"

    info "Checking out $TARGET..."
    git checkout "$TARGET"

    info "Resetting to origin/$TARGET..."
    git reset --hard origin/"$TARGET"

    info "Pull complete. Working tree is now at origin/$TARGET."
    info "Run bootstrap again to apply a deployment."
    exit 0
fi

# --- Fetch deployment scripts ---
DEPLOY_DIR="/tmp/pifitness-deploy"
mkdir -p "$DEPLOY_DIR"

info "Fetching latest deployment scripts from origin/deployment_script..."
if ! git -C "$PROJECT_DIR" fetch origin deployment_script 2>/dev/null; then
    info "Using local deployment scripts (could not fetch deployment_script branch)"
    SCRIPT_DIR="$PROJECT_DIR/deployment"
else
    SCRIPT_DIR="$DEPLOY_DIR"
    info "Extracting deployment scripts from deployment_script branch..."
    if git -C "$PROJECT_DIR" archive origin/deployment_script deployment/ | tar -x -C "$DEPLOY_DIR" --strip-components=1 2>/dev/null; then
        chmod +x "$DEPLOY_DIR"/*.sh 2>/dev/null || true
    else
        warn "Failed to extract deployment scripts. Falling back to local scripts."
        SCRIPT_DIR="$PROJECT_DIR/deployment"
    fi
fi

# --- Execute the React deployment script ---
TARGET_SCRIPT="$SCRIPT_DIR/deploy_react.sh"
if [[ ! -f "$TARGET_SCRIPT" ]]; then
    error_exit "Deployment script not found: $TARGET_SCRIPT"
fi

info "Executing: bash $TARGET_SCRIPT --install-packages=$INSTALL_PACKAGES --fast=$FAST --nuclear=$NUCLEAR"
echo ""
echo "========================================="
echo "  Starting deployment of $TARGET"
echo "========================================="
echo ""

# Suspend set -e so we can capture the exit code and print a failure banner.
# Otherwise set -e would exit the script before the if/else block runs.
set +e
bash "$TARGET_SCRIPT" --install-packages="$INSTALL_PACKAGES" --fast="$FAST" --nuclear="$NUCLEAR"
DEPLOY_EXIT_CODE=$?
set -e

if [[ $DEPLOY_EXIT_CODE -eq 0 ]]; then
    echo ""
    echo "========================================="
    echo "  Deployment of $TARGET completed successfully"
    echo "========================================="
    echo ""
    exit 0
else
    echo ""
    echo "========================================="
    echo "  Deployment FAILED (exit code: $DEPLOY_EXIT_CODE)"
    echo "========================================="
    echo ""
    exit $DEPLOY_EXIT_CODE
fi