#!/bin/bash
# PiFitness Bootstrap Launcher
# Usage: bash bootstrap.sh [options]
#
# Modes (interactive or via flags):
#   1) Full deploy          --install-packages   (or run with no flags + menu option 1)
#   2) Fast deploy          --fast
#   3) Nuclear              --nuclear
#   4) Update bootstrap     --update-bootstrap   (pull deployment/ from repo,
#                                                 copy bootstrap.sh + deploy_react.sh
#                                                 to /home/god/Documents/, no deploy)
#
# Flow:
#   1. (Modes 1-3) Validates environment, fetches deployment scripts from
#      origin/deployment_script, runs deploy_react.sh
#   2. (Mode 4) Fetches deployment/ from origin/deployment_script and updates
#      the copies in /home/god/Documents/

set -e

# --- Constants ---
PROJECT_DIR="/home/god/PiFitness"
TARGET="react-ui"                    # Only one deployment target now
BOOTSTRAP_DIR="/home/god/Documents"  # Where the user keeps their bootstrap copy
DEPLOY_BRANCH="deployment_script"    # Branch that holds deployment/ in the repo

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
UPDATE_BOOTSTRAP=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --install-packages)
            INSTALL_PACKAGES=true
            shift
            ;;
        --fast)
            FAST=true
            NUCLEAR=false
            UPDATE_BOOTSTRAP=false
            shift
            ;;
        --nuclear)
            NUCLEAR=true
            FAST=false
            UPDATE_BOOTSTRAP=false
            shift
            ;;
        --update-bootstrap)
            UPDATE_BOOTSTRAP=true
            FAST=false
            NUCLEAR=false
            shift
            ;;
        *)
            error_exit "Unknown argument: $1"
            ;;
    esac
done

# --- Interactive prompt if no mode was given ---
if [[ -z "$FAST" && -z "$NUCLEAR" && -z "$UPDATE_BOOTSTRAP" ]]; then
    echo ""
    echo "Select deployment mode:"
    echo "  1) Full deploy (pull code, tests, install packages, rebuild, restart services)"
    echo "  2) Fast deploy (pull code, skip tests/packages, restart services only)"
    echo "  3) Nuclear (wipe everything, re-clone, rebuild from scratch)"
    echo "  4) Update bootstrap (pull latest deployment/ files from repo)"
    read -rp "Enter 1, 2, 3, or 4: " mode_choice

    case $mode_choice in
        1) FAST=false; NUCLEAR=false; UPDATE_BOOTSTRAP=false ;;
        2) FAST=true;  NUCLEAR=false; UPDATE_BOOTSTRAP=false ;;
        3) FAST=false; NUCLEAR=true;  UPDATE_BOOTSTRAP=false ;;
        4) FAST=false; NUCLEAR=false; UPDATE_BOOTSTRAP=true  ;;
        *) error_exit "Invalid choice. Exiting." ;;
    esac

    if [[ "$FAST" != "true" && "$NUCLEAR" != "true" && "$UPDATE_BOOTSTRAP" != "true" ]]; then
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

# Defaults for unset values
INSTALL_PACKAGES="${INSTALL_PACKAGES:-false}"
FAST="${FAST:-false}"
NUCLEAR="${NUCLEAR:-false}"
UPDATE_BOOTSTRAP="${UPDATE_BOOTSTRAP:-false}"

# Sanity checks
if [[ "$FAST" == true && "$NUCLEAR" == true ]]; then
    error_exit "--fast and --nuclear are mutually exclusive"
fi
if [[ "$UPDATE_BOOTSTRAP" == true && ("$FAST" == true || "$NUCLEAR" == true) ]]; then
    error_exit "--update-bootstrap cannot be combined with --fast or --nuclear"
fi

info "Target: $TARGET"
info "Options: install-packages=$INSTALL_PACKAGES fast=$FAST nuclear=$NUCLEAR update-bootstrap=$UPDATE_BOOTSTRAP"

# ======================================================================
# Mode 4: Update bootstrap (runs before pre-flight checks so it works
# even when the project directory is broken)
# ======================================================================
if [[ "$UPDATE_BOOTSTRAP" == "true" ]]; then
    info "=== UPDATE BOOTSTRAP MODE ==="

    if ! command -v git &> /dev/null; then
        error_exit "Git is not installed"
    fi

    EXTRACT_DIR="/tmp/pifitness-bootstrap-update"
    rm -rf "$EXTRACT_DIR"
    mkdir -p "$EXTRACT_DIR"

    if [[ -d "$PROJECT_DIR/.git" ]]; then
        info "Fetching origin/$DEPLOY_BRANCH..."
        if ! git -C "$PROJECT_DIR" fetch origin "$DEPLOY_BRANCH"; then
            error_exit "Failed to fetch origin/$DEPLOY_BRANCH"
        fi
        info "Extracting deployment/ from origin/$DEPLOY_BRANCH..."
        if ! git -C "$PROJECT_DIR" archive "origin/$DEPLOY_BRANCH" deployment/ \
             | tar -x -C "$EXTRACT_DIR" --strip-components=1 2>/dev/null; then
            error_exit "Failed to extract deployment/ from origin/$DEPLOY_BRANCH"
        fi
    else
        warn "$PROJECT_DIR is not a git repo; using a temporary clone"
        TEMP_CLONE="/tmp/pifitness-bootstrap-src"
        rm -rf "$TEMP_CLONE"
        if ! git clone --depth 1 --branch "$DEPLOY_BRANCH" \
             https://github.com/patrick-britton/PiFitness.git "$TEMP_CLONE"; then
            error_exit "Failed to clone $DEPLOY_BRANCH from origin"
        fi
        cp -r "$TEMP_CLONE/deployment/." "$EXTRACT_DIR/"
        rm -rf "$TEMP_CLONE"
    fi

    if [[ ! -f "$EXTRACT_DIR/bootstrap.sh" ]]; then
        error_exit "bootstrap.sh not found in extracted deployment/ — aborting"
    fi

    info "Updating bootstrap files in $BOOTSTRAP_DIR/..."
    install -m 755 "$EXTRACT_DIR/bootstrap.sh"   "$BOOTSTRAP_DIR/bootstrap.sh"

    if [[ -f "$EXTRACT_DIR/deploy_react.sh" ]]; then
        install -m 755 "$EXTRACT_DIR/deploy_react.sh" "$BOOTSTRAP_DIR/deploy_react.sh"
    fi

    rm -rf "$EXTRACT_DIR"

    info "Update complete:"
    info "  $BOOTSTRAP_DIR/bootstrap.sh"
    [[ -f "$BOOTSTRAP_DIR/deploy_react.sh" ]] && info "  $BOOTSTRAP_DIR/deploy_react.sh"
    info ""
    info "Re-run the bootstrap to use the new version."
    exit 0
fi

# ======================================================================
# Pre-flight checks (modes 1-3)
# ======================================================================
info "Running pre-flight checks..."

# Disk space (at least 2 GB free)
AVAILABLE_SPACE=$(df /home/god --output=avail 2>/dev/null | tail -1)
if [[ -z "$AVAILABLE_SPACE" ]]; then
    warn "Could not check disk space. Continuing anyway."
elif [[ "$AVAILABLE_SPACE" -lt 2097152 ]]; then
    error_exit "Insufficient disk space: ${AVAILABLE_SPACE}KB available, need at least 2GB"
fi

# Master .env exists
if [[ ! -f "$BOOTSTRAP_DIR/.env" ]]; then
    warn "Master .env not found at $BOOTSTRAP_DIR/.env"
    warn "Deployment will continue, but services may fail without environment variables"
fi

# Git available
if ! command -v git &> /dev/null; then
    error_exit "Git is not installed"
fi

# Venv exists (skip for nuclear)
VENV_DIR="$PROJECT_DIR/venv"
if [[ ! -d "$VENV_DIR" && "$NUCLEAR" == false ]]; then
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

# ======================================================================
# Fetch deployment scripts for the actual deploy
# ======================================================================
DEPLOY_DIR="/tmp/pifitness-deploy"
rm -rf "$DEPLOY_DIR"
mkdir -p "$DEPLOY_DIR"

info "Fetching latest deployment scripts from origin/$DEPLOY_BRANCH..."
if ! git -C "$PROJECT_DIR" fetch origin "$DEPLOY_BRANCH" 2>/dev/null; then
    info "Using local deployment scripts (could not fetch $DEPLOY_BRANCH)"
    SCRIPT_DIR="$PROJECT_DIR/deployment"
else
    SCRIPT_DIR="$DEPLOY_DIR"
    info "Extracting deployment scripts from origin/$DEPLOY_BRANCH..."
    if git -C "$PROJECT_DIR" archive "origin/$DEPLOY_BRANCH" deployment/ \
         | tar -x -C "$DEPLOY_DIR" --strip-components=1 2>/dev/null; then
        chmod +x "$DEPLOY_DIR"/*.sh 2>/dev/null || true
    else
        warn "Failed to extract deployment scripts. Falling back to local scripts."
        SCRIPT_DIR="$PROJECT_DIR/deployment"
    fi
fi

# ======================================================================
# Execute the React deployment script
# ======================================================================
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