#!/bin/bash
# PiFitness Bootstrap Launcher
# Usage: bash bootstrap.sh [options]
#   Options override interactive prompts.
#
# This script:
#   1. Validates environment
#   2. Fetches deployment scripts from origin/deployment_script
#   3. Runs the target-specific deployment script
#
# This script lives on the deployment_script branch only.
# It is NOT coupled to any app branch.

set -e

# --- Constants ---
PROJECT_DIR="/home/god/PiFitness"

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

# --- Parse CLI args (optional — if omitted, interactive prompts are shown) ---
TARGET=""
INSTALL_PACKAGES=""
FAST=""
NUCLEAR=""

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
            NUCLEAR=false
            shift
            ;;
        --nuclear)
            NUCLEAR=true
            FAST=false
            shift
            ;;
        *)
            error_exit "Unknown argument: $1"
            ;;
    esac
done

# --- Interactive prompts if not all args provided ---
if [[ -z "$TARGET" ]]; then
    echo ""
    echo "Which branch to deploy?"
    echo "  1) streamlit-prd (legacy Streamlit)"
    echo "  2) react-ui (new FastAPI + React)"
    read -rp "Enter 1 or 2: " branch_choice

    case $branch_choice in
        1) TARGET="streamlit-prd" ;;
        2) TARGET="react-ui" ;;
        *) error_exit "Invalid choice. Exiting." ;;
    esac
fi

if [[ -z "$FAST" && -z "$NUCLEAR" ]]; then
    echo ""
    echo "Select deployment mode:"
    echo "  1) Full deploy (pull code, tests, install packages, rebuild, restart services)"
    echo "  2) Fast deploy (pull code, skip tests/packages, restart services only)"
    echo "  3) Nuclear (wipe everything, re-clone, rebuild from scratch)"
    read -rp "Enter 1, 2, or 3: " mode_choice

    case $mode_choice in
        1) FAST=false; NUCLEAR=false ;;
        2) FAST=true; NUCLEAR=false ;;
        3) FAST=false; NUCLEAR=true ;;
        *) error_exit "Invalid choice. Exiting." ;;
    esac

    # Ask about package install unless fast (which skips packages)
    if [[ "$FAST" != "true" && "$NUCLEAR" != "true" ]]; then
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
VENV_DIR="$PROJECT_DIR/venv"
if [[ ! -d "$VENV_DIR" && "$NUCLEAR" == false ]]; then
    warn "Virtual environment not found at $VENV_DIR"
    warn "Will create it during deployment"
fi

# Check target branch exists (run git from PROJECT_DIR regardless of CWD)
if git -C "$PROJECT_DIR" show-ref --verify --quiet "refs/heads/$TARGET" 2>/dev/null; then
    info "Target branch '$TARGET' exists locally"
else
    warn "Target branch '$TARGET' not found locally. Will fetch from origin."
fi

info "Pre-flight checks passed."

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
    # Use git archive to get the entire deployment/ directory
    if git -C "$PROJECT_DIR" archive origin/deployment_script deployment/ | tar -x -C "$DEPLOY_DIR" --strip-components=1 2>/dev/null; then
        chmod +x "$DEPLOY_DIR"/*.sh 2>/dev/null || true
    else
        warn "Failed to extract deployment scripts. Falling back to local scripts."
        SCRIPT_DIR="$PROJECT_DIR/deployment"
    fi
fi

# --- Execute target-specific script ---
# Map branch names to script names
if [[ "$TARGET" == "streamlit-prd" ]]; then
    TARGET_SHORT="streamlit"
elif [[ "$TARGET" == "react-ui" ]]; then
    TARGET_SHORT="react"
fi
TARGET_SCRIPT="$SCRIPT_DIR/deploy_${TARGET_SHORT}.sh"
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
    if [[ "$TARGET" == "react-ui" ]]; then
        info "  bash bootstrap.sh streamlit-prd"
    else
        info "  bash bootstrap.sh react-ui"
    fi
else
    echo ""
    echo "========================================="
    echo "  Deployment FAILED (exit code: $DEPLOY_EXIT_CODE)"
    echo "========================================="
    echo ""
    warn "To roll back to the last working version, run:"
    if [[ "$TARGET" == "react-ui" ]]; then
        warn "  bash bootstrap.sh streamlit-prd"
    else
        warn "  bash bootstrap.sh react-ui"
    fi
    exit $DEPLOY_EXIT_CODE
fi