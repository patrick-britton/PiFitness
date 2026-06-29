#!/bin/bash
# PiFitness Streamlit Deployment Script
# Called by bootstrap.sh -- target: streamlit-prd
#
# This script handles deployment of the frozen streamlit-prd branch.
# It NEVER changes -- only the deployment_script branch updates it.
#
# Usage (called from bootstrap.sh):
#   deploy_streamlit.sh --install-packages=true/false --fast=true/false --nuclear=true/false

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

# --- Parse args ---
INSTALL_PACKAGES=false
FAST=false
NUCLEAR=false
TARGET="streamlit-prd"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --install-packages=*) INSTALL_PACKAGES="${1#*=}" ;;
        --fast=*) FAST="${1#*=}" ;;
        --nuclear=*) NUCLEAR="${1#*=}" ;;
        *) error_exit "Unknown argument: $1" ;;
    esac
    shift
done

PROJECT_DIR="/home/god/PiFitness"
VENV_DIR="$PROJECT_DIR/venv"
REQUIREMENTS_FILE="requirements-streamlit.txt"

# ======================================================================
# NUCLEAR MODE: Full wipe and re-clone
# ======================================================================
if [[ "$NUCLEAR" == "true" ]]; then
    info "=== NUCLEAR MODE ==="
    echo ""
    echo "WARNING: This will DELETE $PROJECT_DIR, the venv, npm caches, and re-clone."
    read -rp "Continue? [y/N]: " confirm
    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
        info "Nuclear mode cancelled."
        exit 1
    fi

    # Backup data first
    info "Backing up database..."
    mkdir -p /home/god/DB-Backups
    pg_dump -h localhost -U god personal_fitness > "/home/god/DB-Backups/pre-nuclear-$(date +%Y%m%d-%H%M%S).sql" || warn "DB backup failed"

    info "Backing up .env..."
    cp /home/god/Documents/.env /home/god/Documents/.env.pre-nuclear 2>/dev/null || true

    info "Backing up auth tokens..."
    for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
        [ -f "$PROJECT_DIR/$token_file" ] && cp "$PROJECT_DIR/$token_file" "/tmp/${token_file}.backup" || true
    done

    # Wipe everything
    info "Wiping project directory, venv, and npm caches..."
    rm -rf "$PROJECT_DIR"
    rm -rf "$VENV_DIR"
    rm -rf ~/.npm/_cacache

    # Re-clone
    info "Cloning repository..."
    git clone https://github.com/patrick-britton/PiFitness.git "$PROJECT_DIR"
    cd "$PROJECT_DIR"
    git fetch origin
    git checkout "$TARGET"

    # Restore .env
    cp /home/god/Documents/.env "$PROJECT_DIR/.env"

    # Restore auth tokens
    for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
        [ -f "/tmp/${token_file}.backup" ] && cp "/tmp/${token_file}.backup" "$PROJECT_DIR/$token_file" || true
    done

    # Create venv
    info "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"

    # Install Python packages
    info "Installing Python dependencies..."
    pip install -r "$PROJECT_DIR/deployment/$REQUIREMENTS_FILE"

    # Start services
    info "Starting services..."
    sudo cp "$PROJECT_DIR/deployment/pifitness-streamlit.service" /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl start pifitness-streamlit.service

    # Configure nginx for port 8501
    NGINX_TEMPLATE="$PROJECT_DIR/deployment/nginx-template.conf"
    NGINX_SITE="/etc/nginx/sites-available/pifitness"
    sudo sed "s/FASTAPI_PORT/8501/g" "$NGINX_TEMPLATE" | sudo tee "$NGINX_SITE" > /dev/null
    if [[ ! -f "/etc/nginx/sites-enabled/pifitness" ]]; then
        sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness
    fi
    sudo rm -f /etc/nginx/sites-enabled/streamlit 2>/dev/null || true
    sudo nginx -t && sudo systemctl reload nginx

    # Start agent timer
    sudo systemctl start pifitness_agent.timer 2>/dev/null || warn "Agent timer not available"

    info "Nuclear deployment of streamlit-prd completed."
    exit 0
fi

# ======================================================================
# FAST MODE: git pull + minimal restart
# ======================================================================
if [[ "$FAST" == "true" ]]; then
    info "=== FAST MODE ==="
    cd "$PROJECT_DIR"

    # Fetch latest from origin
    git fetch origin

    # Ensure we're on the right branch (switch if needed)
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$CURRENT_BRANCH" != "$TARGET" ]]; then
        info "Switching from '$CURRENT_BRANCH' to '$TARGET'..."
        # Force checkout to discard any local changes from the previous branch
        git checkout --force "$TARGET"
    fi

    # Check what changed since last deploy
    CHANGED_FILES=$(git diff HEAD..origin/"$TARGET" --name-only)

    if [[ -z "$CHANGED_FILES" ]]; then
        info "No code changes detected. Verifying services are running..."
        # Ensure Streamlit service is running (may have been stopped after branch switch)
        if ! sudo systemctl is-active --quiet pifitness-streamlit.service; then
            info "Streamlit service not running. Starting it..."
            sudo systemctl start pifitness-streamlit.service
            # Ensure nginx is configured for streamlit
            NGINX_TEMPLATE="$PROJECT_DIR/deployment/nginx-template.conf"
            NGINX_SITE="/etc/nginx/sites-available/pifitness"
            if [[ -f "$NGINX_TEMPLATE" ]]; then
                sudo sed "s/FASTAPI_PORT/8501/g" "$NGINX_TEMPLATE" | sudo tee "$NGINX_SITE" > /dev/null 2>/dev/null || true
                sudo nginx -t 2>/dev/null && sudo systemctl reload nginx 2>/dev/null || true
            fi
            info "Streamlit service started."
        else
            info "Streamlit service already running."
        fi
        exit 0
    fi

    info "Changed files:"
    echo "$CHANGED_FILES"

    # Reset to match origin exactly (safer than pull which can fail on diverged branches)
    git reset --hard origin/"$TARGET"

    # Purge Python bytecode
    find "$PROJECT_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find "$PROJECT_DIR" -type f -name "*.pyc" -exec rm -f {} + 2>/dev/null || true

    # Check if only non-app files changed
    ONLY_CONFIG=true
    for f in $CHANGED_FILES; do
        case "$f" in
            deployment/*|scripts/*|tests/*|README*|.gitignore|*.md)
                ;;
            *)
                ONLY_CONFIG=false
                ;;
        esac
    done

    if [[ "$ONLY_CONFIG" == "true" ]]; then
        info "Only config/docs changed. No service restart needed."
        exit 0
    fi

    # Copy .env (in case it was reset)
    cp /home/god/Documents/.env "$PROJECT_DIR/.env" 2>/dev/null || true

    # Restart Streamlit
    info "Restarting Streamlit service..."
    sudo systemctl stop pifitness-streamlit.service 2>/dev/null || true
    sleep 1
    sudo systemctl start pifitness-streamlit.service

    info "Fast deployment of streamlit-prd completed."
    exit 0
fi

# ======================================================================
# FULL DEPLOY MODE (default)
# ======================================================================
info "=== FULL DEPLOY MODE ==="

# --- 1. Stop all services ---
info "Stopping any running services..."
rm -f /tmp/*.sock /tmp/*.pid 2>/dev/null || true

# Stop agent
sudo systemctl stop pifitness_agent.service 2>/dev/null || true
sudo systemctl stop pifitness_agent.timer 2>/dev/null || true

# Stop app processes
sudo systemctl stop pifitness-streamlit.service 2>/dev/null || true
sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
sleep 2

# Force-kill lingering
pkill -9 -f "streamlit run" 2>/dev/null || true
pkill -9 -f "uvicorn" 2>/dev/null || true
for port in 8000 8501 3000; do
    lsof -ti :$port 2>/dev/null | xargs -r kill -9 2>/dev/null || true
done

# --- 2. Purge caches ---
info "Purging caches..."
find "$PROJECT_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$PROJECT_DIR" -type f -name "*.pyc" -exec rm -f {} + 2>/dev/null || true
sudo rm -rf /var/cache/nginx/* 2>/dev/null || true

# --- 3. Back up local config ---
info "Backing up local configuration..."
cd "$PROJECT_DIR"

# .env is backed up from master location, but also save project-local copies
[ -f .env ] && cp .env /tmp/.env.streamlit.backup || true

# Auth tokens
for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
    [ -f "$PROJECT_DIR/$token_file" ] && cp "$PROJECT_DIR/$token_file" "/tmp/${token_file}.backup" || true
done

# --- 4. Pull the branch ---
info "Pulling branch: $TARGET"
git fetch origin
git reset --hard origin/"$TARGET"

# --- 5. Restore config ---
info "Restoring configuration..."
cp /home/god/Documents/.env "$PROJECT_DIR/.env" 2>/dev/null || cp /tmp/.env.streamlit.backup "$PROJECT_DIR/.env" 2>/dev/null || true
for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
    [ -f "/tmp/${token_file}.backup" ] && cp "/tmp/${token_file}.backup" "$PROJECT_DIR/$token_file" || true
done

# --- 6. Install Python dependencies ---
source "$VENV_DIR/bin/activate"

if [[ "$INSTALL_PACKAGES" == "true" ]]; then
    info "Installing/updating Python dependencies..."
    pip install -r "$PROJECT_DIR/deployment/$REQUIREMENTS_FILE"
else
    info "Skipping Python package checks"
fi

# --- 7. Run tests ---
info "Running automated tests..."
if ! pytest "$PROJECT_DIR/tests/" -v; then
    error_exit "Tests failed, aborting deployment"
fi
info "All tests passed."

# --- 8. Install systemd service files ---
info "Installing systemd service files..."
sudo cp "$PROJECT_DIR/deployment/pifitness-streamlit.service" /etc/systemd/system/
sudo cp "$PROJECT_DIR/deployment/pifitness-fastapi.service" /etc/systemd/system/
sudo systemctl daemon-reload

# --- 9. Start Streamlit service ---
info "Starting Streamlit service..."
sudo systemctl start pifitness-streamlit.service
TARGET_PORT=8501

# --- 10. Update nginx configuration ---
info "Updating nginx configuration..."
NGINX_TEMPLATE="$PROJECT_DIR/deployment/nginx-template.conf"
NGINX_SITE="/etc/nginx/sites-available/pifitness"

if [[ ! -f "$NGINX_TEMPLATE" ]]; then
    error_exit "Nginx template not found at $NGINX_TEMPLATE"
fi

sudo sed "s/FASTAPI_PORT/${TARGET_PORT}/g" "$NGINX_TEMPLATE" | sudo tee "$NGINX_SITE" > /dev/null

if ! grep -q ":${TARGET_PORT};" "$NGINX_SITE"; then
    error_exit "Failed to update nginx configuration with port ${TARGET_PORT}"
fi

if [[ ! -f "/etc/nginx/sites-enabled/pifitness" ]]; then
    sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness
fi

# Remove stale streamlit symlink if it exists
sudo rm -f /etc/nginx/sites-enabled/streamlit 2>/dev/null || true

info "Testing nginx configuration..."
sudo nginx -t || error_exit "Nginx configuration test failed."

info "Reloading nginx..."
sudo systemctl reload nginx || error_exit "Failed to reload nginx."

# --- 11. Restart agent service ---
info "Restarting agent service..."
sudo systemctl start pifitness_agent.timer 2>/dev/null || warn "Agent timer not available"

# --- 12. Cleanup old backups (keep last 2) ---
cd /home/god/PiFitness/backups 2>/dev/null && ls -1t | tail -n +3 | xargs -r rm -rf 2>/dev/null || true

info "Deployment of streamlit-prd completed successfully!"