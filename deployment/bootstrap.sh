#!/bin/bash
# PiFitness Bootstrap Launcher
# Usage: bash bootstrap.sh <target> [--install-packages] [--nuclear] [--fast]
#   target: streamlit-prd or react-ui
#
# This script:
#   1. Validates environment
#   2. Fetches deployment scripts from origin/deployment_script
#   3. Runs the target-specific deployment script
#
# This script lives on the deployment_script branch only.
# It is NOT coupled to any app branch.

set -e

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

usage() {
    echo "Usage: bash bootstrap.sh <target> [options]"
    echo ""
    echo "  target: streamlit-prd | react-ui"
    echo ""
    echo "Options:"
    echo "  --install-packages    Install/verify all Python & npm dependencies"
    echo "  --fast                Partial deploy (git pull only, skip package install, skip DB backup)"
    echo "  --nuclear             Full wipe: delete project dir, venv, npm caches, re-clone, rebuild"
    echo ""
    echo "Examples:"
    echo "  bash bootstrap.sh react-ui --install-packages"
    echo "  bash bootstrap.sh react-ui --fast"
    echo "  bash bootstrap.sh streamlit-prd --install-packages"
    echo "  bash bootstrap.sh react-ui --nuclear"
    exit 1
}

# --- Parse args ---
TARGET=""
INSTALL_PACKAGES=false
FAST=false
NUCLEAR=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        streamlit-prd|react-ui)
            TARGET="$1"
            shift
            ;;
        --install-packages)
            INSTALL_PACKAGES=true
            shift
            ;;
        --fast)
            FAST=true
            shift
            ;;
        --nuclear)
            NUCLEAR=true
            shift
            ;;
        *)
            error_exit "Unknown argument: $1"
            ;;
    esac
done

if [[ -z "$TARGET" ]]; then
    usage
fi

if [[ "$FAST" == true && "$NUCLEAR" == true ]]; then
    error_exit "--fast and --nuclear are mutually exclusive"
fi

if [[ "$TARGET" != "streamlit-prd" && "$TARGET" != "react-ui" ]]; then
    error_exit "Target must be 'streamlit-prd' or 'react-ui'"
fi

info "Target: $TARGET"
info "Options: install-packages=$INSTALL_PACKAGES fast=$FAST nuclear=$NUCLEAR"

# --- Pre-flight checks ---
info "Running pre-flight checks..."

# Check disk space (need at least 2GB free)
AVAILABLE_SPACE=$(df /home/god --output=avail 2>/dev/null | tail -1)
if [[ -z "$AVAILABLE_SPACE" ]]; then
    warn "Could not check disk space. Continuing anyway."
elif [[ "$AVAILABLE_SPACE" -lt 2097152 ]]; then
    error_exit "Insufficient disk space: ${AVAILABLE_SPACE}KB available, need at least 2GB"
fi

# Check .env master copy exists
if [[ ! -f "/home/god/Documents/.env" ]]; then
    warn "Master .env not found at /home/god/Documents/.env"
    warn "Deployment will continue, but services may fail without environment variables"
fi

# Check git is available
if ! command -v git &> /dev/null; then
    error_exit "Git is not installed"
fi

# Check Python virtual environment
VENV_DIR="/home/god/PiFitness/venv"
if [[ ! -d "$VENV_DIR" && "$NUCLEAR" == false ]]; then
    warn "Virtual environment not found at $VENV_DIR"
    warn "Will create it during deployment"
fi

# Check target branch exists
if git show-ref --verify --quiet "refs/heads/$TARGET" 2>/dev/null; then
    info "Target branch '$TARGET' exists locally"
else
    warn "Target branch '$TARGET' not found locally. Will fetch from origin."
fi

info "Pre-flight checks passed."

# --- Fetch deployment scripts ---
DEPLOY_DIR="/tmp/pifitness-deploy"
mkdir -p "$DEPLOY_DIR"

info "Fetching latest deployment scripts from origin/deployment_script..."
if ! git fetch origin deployment_script 2>/dev/null; then
    info "Using local deployment scripts (could not fetch deployment_script branch)"
    SCRIPT_DIR="/home/god/PiFitness/deployment"
else
    SCRIPT_DIR="$DEPLOY_DIR"
    # Extract files from deployment_script branch
    for f in bootstrap.sh deploy_streamlit.sh deploy_react.sh nginx-template.conf requirements-streamlit.txt requirements-react.txt npm_requirements.txt; do
        git show origin/deployment_script:"deployment/$f" > "$DEPLOY_DIR/$f" 2>/dev/null || warn "Could not extract $f from deployment_script branch"
    done
    chmod +x "$DEPLOY_DIR"/*.sh 2>/dev/null || true
fi

# --- Pre-deployment validation (dry-run mode if --fast or --install-packages) ---
if [[ "$FAST" == false && "$NUCLEAR" == false ]]; then
    info "Creating database backup..."
    BACKUP_DIR="/home/god/DB-Backups"
    mkdir -p "$BACKUP_DIR"
    BACKUP_FILE="$BACKUP_DIR/pifitness-$(date +%Y%m%d-%H%M%S).sql"
    if pg_dump -h localhost -U god personal_fitness > "$BACKUP_FILE" 2>/dev/null; then
        info "Database backup saved to $BACKUP_FILE"
    else
        warn "Database backup failed (continuing anyway)"
    fi
fi

# --- Execute target-specific script ---
TARGET_SCRIPT="$SCRIPT_DIR/deploy_${TARGET//-/_}.sh"
if [[ ! -f "$TARGET_SCRIPT" ]]; then
    error_exit "Deployment script not found: $TARGET_SCRIPT"
fi

info "Executing: bash $TARGET_SCRIPT --install-packages=$INSTALL_PACKAGES --fast=$FAST --nuclear=$NUCLEAR"
echo ""
echo "========================================="
echo "  Starting deployment of $TARGET"
echo "========================================="
echo ""

bash "$TARGET_SCRIPT" --install-packages="$INSTALL_PACKAGES" --fast="$FAST" --nuclear="$NUCLEAR"

DEPLOY_EXIT_CODE=$?

if [[ $DEPLOY_EXIT_CODE -eq 0 ]]; then
    echo ""
    echo "========================================="
    echo "  Deployment of $TARGET completed successfully"
    echo "========================================="
    echo ""
    info "To roll back to the other version, run:"
    info "  bash bootstrap.sh <other_target> --install-packages"
else
    echo ""
    echo "========================================="
    echo "  Deployment FAILED (exit code: $DEPLOY_EXIT_CODE)"
    echo "========================================="
    echo ""
    warn "To roll back to the last working version, run:"
    if [[ "$TARGET" == "react-ui" ]]; then
        warn "  bash bootstrap.sh streamlit-prd --install-packages"
    else
        warn "  bash bootstrap.sh react-ui --install-packages"
    fi
    exit $DEPLOY_EXIT_CODE
fi