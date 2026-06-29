#!/bin/bash
# PiFitness React Deployment Script
# Called by bootstrap.sh -- target: react-ui
#
# This script handles deployment of the react-ui branch.
# It evolves as the migration progresses.
#
# Usage (called from bootstrap.sh):
#   deploy_react.sh --install-packages=true/false --fast=true/false --nuclear=true/false

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
TARGET="react-ui"

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
FRONTEND_DIR="$PROJECT_DIR/frontend/pifitness"
REQUIREMENTS_FILE="requirements-react.txt"
NPM_REQUIREMENTS_FILE="npm_requirements.txt"

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

    # Backup .env and auth tokens (no database backup – database is untouched)
    info "Backing up .env..."
    [ -f "$PROJECT_DIR/backend/.env" ] && cp "$PROJECT_DIR/backend/.env" /home/god/Documents/.env.pre-nuclear
    cp /home/god/Documents/.env /home/god/Documents/.env.pre-nuclear 2>/dev/null || true

    info "Backing up auth tokens..."
    for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
        [ -f "$PROJECT_DIR/$token_file" ] && cp "$PROJECT_DIR/$token_file" "/tmp/${token_file}.backup" || true
    done

     # Wipe everything
     info "Stopping any running services before wipe..."
     sudo systemctl stop pifitness-streamlit.service 2>/dev/null || true
     sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
     pm2 delete pifitness-next 2>/dev/null || true
     # Clear PM2 saved state to purge any stale environment variables
     rm -f ~/.pm2/dump.pm2
     for port in 8000 8501 3000; do
         lsof -ti :$port 2>/dev/null | xargs -r kill -9 2>/dev/null || true
     done
     info "Wiping project directory, venv, and npm caches..."
     rm -rf "$PROJECT_DIR"
     rm -rf "$VENV_DIR"
     rm -rf ~/.npm/_cacache
     # Also wipe PM2's entire cache to be absolutely clean
     rm -rf ~/.pm2

    # Re-clone
    info "Cloning repository..."
    git clone https://github.com/patrick-britton/PiFitness.git "$PROJECT_DIR"
    cd "$PROJECT_DIR"
    git fetch origin
    git checkout "$TARGET"

    # Restore .env (react uses backend/.env AND project root for systemd service)
    cp /home/god/Documents/.env "$PROJECT_DIR/backend/.env"
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

    # Install npm packages
    info "Installing npm packages..."
    cd "$FRONTEND_DIR"
    npm install --no-save 2>/dev/null || true
    # Also install from requirements file if it exists
    if [[ -f "$PROJECT_DIR/deployment/$NPM_REQUIREMENTS_FILE" ]]; then
        while IFS= read -r package || [[ -n "$package" ]]; do
            if [[ -n "$package" && "$package" != \#* ]]; then
                npm install --no-save "$package" 2>/dev/null || true
            fi
        done < "$PROJECT_DIR/deployment/$NPM_REQUIREMENTS_FILE"
    fi

    # Build frontend
    info "Building Next.js application..."
    npm run build

    # Start services
    info "Starting services..."
    sudo cp "$PROJECT_DIR/deployment/pifitness-fastapi.service" /etc/systemd/system/
    sudo cp "$PROJECT_DIR/deployment/pifitness-streamlit.service" /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl start pifitness-fastapi.service

    # Start Next.js with PM2 (fresh environment)
    if ! command -v pm2 &> /dev/null; then
        sudo npm install -g pm2
    fi
    # Environment already clean, but we ensure no stale PM2 data
    pm2 kill 2>/dev/null || true
    rm -rf ~/.pm2
    # Purge environment variables to prevent dev proxy leaks
    unset NEXT_PUBLIC_API_URL
    export NEXT_PUBLIC_APP_ENV=production
    pm2 start npm --name pifitness-next -- run start -- --port 3000
    pm2 save --force

    # Configure nginx using the dedicated React config (ports 8000 & 3000 already baked in)
    NGINX_TEMPLATE="$PROJECT_DIR/deployment/nginx-react.conf"
    NGINX_SITE="/etc/nginx/sites-available/pifitness"
    sudo cp "$NGINX_TEMPLATE" "$NGINX_SITE"
    if [[ ! -f "/etc/nginx/sites-enabled/pifitness" ]]; then
        sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness
    fi
    sudo rm -f /etc/nginx/sites-enabled/streamlit 2>/dev/null || true
    if ! sudo nginx -t; then
        error_exit "Nginx configuration test failed!"
    fi
    sudo systemctl reload nginx || error_exit "Failed to reload nginx."

    # Start agent timer
    sudo systemctl start pifitness_agent.timer 2>/dev/null || warn "Agent timer not available"

    info "Nuclear deployment of react-ui completed."
    exit 0
fi

# ======================================================================
# FAST MODE: git pull + selective restart
# ======================================================================
if [[ "$FAST" == "true" ]]; then
    info "=== FAST MODE ==="

    # Ensure any previous Streamlit deployment is shut down
    info "Stopping any lingering Streamlit services..."
    sudo systemctl stop pifitness-streamlit.service 2>/dev/null || true
    lsof -ti :8501 2>/dev/null | xargs -r kill -9 2>/dev/null || true

    cd "$PROJECT_DIR"

    # Fetch latest from origin
    git fetch origin

    # Ensure we're on the right branch (switch if needed)
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$CURRENT_BRANCH" != "$TARGET" ]]; then
        info "Switching from '$CURRENT_BRANCH' to '$TARGET'..."
        git checkout --force "$TARGET"
    fi

    # Force alignment of other components if we just switched branches, even with no code changes
    if [[ "$CURRENT_BRANCH" != "$TARGET" ]]; then
        info "Switching branches. Forcing full restart & nginx realignment..."
        sudo systemctl stop pifitness-streamlit.service 2>/dev/null || true
        
        cp /home/god/Documents/.env "$PROJECT_DIR/backend/.env" 2>/dev/null || true
        
        sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
        sudo systemctl start pifitness-fastapi.service
        
        # Clear PM2 data completely and start fresh
        pm2 delete pifitness-next 2>/dev/null || true
        rm -f ~/.pm2/dump.pm2
        cd "$FRONTEND_DIR"
        unset NEXT_PUBLIC_API_URL
        export NEXT_PUBLIC_APP_ENV=production
        pm2 start npm --name pifitness-next -- run start -- --port 3000
        pm2 save --force
        cd "$PROJECT_DIR"
        
        # Update nginx config
        NGINX_TEMPLATE="$PROJECT_DIR/deployment/nginx-react.conf"
        NGINX_SITE="/etc/nginx/sites-available/pifitness"
        if [[ -f "$NGINX_TEMPLATE" ]]; then
            sudo cp "$NGINX_TEMPLATE" "$NGINX_SITE"
            sudo rm -f /etc/nginx/sites-enabled/streamlit 2>/dev/null || true
            sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness 2>/dev/null || true
            if ! sudo nginx -t; then
                error_exit "Nginx configuration test failed!"
            fi
            sudo systemctl reload nginx || error_exit "Failed to reload nginx."
        fi
        info "Services and nginx realigned for react-ui."
        exit 0
    fi

    # Check what changed since last deploy
    CHANGED_FILES=$(git diff HEAD..origin/"$TARGET" --name-only)

    if [[ -z "$CHANGED_FILES" ]]; then
        info "No code changes detected. Verifying services are running..."
        FASTAPI_ACTIVE=false
        if sudo systemctl is-active --quiet pifitness-fastapi.service; then
            FASTAPI_ACTIVE=true
        fi
        PM2_ACTIVE=false
        if pm2 show pifitness-next 2>/dev/null | grep -q "status.*online"; then
            PM2_ACTIVE=true
        fi

        if [[ "$FASTAPI_ACTIVE" == "true" && "$PM2_ACTIVE" == "true" ]]; then
            info "FastAPI and Next.js services already running."
            NGINX_SITE="/etc/nginx/sites-available/pifitness"
            if [[ -f "$NGINX_SITE" ]] && ! grep -q ":8000;" "$NGINX_SITE"; then
                info "Correcting Nginx routing to port 8000..."
                NGINX_TEMPLATE="$PROJECT_DIR/deployment/nginx-react.conf"
                sudo cp "$NGINX_TEMPLATE" "$NGINX_SITE"
                if ! sudo nginx -t; then
                    error_exit "Nginx configuration test failed!"
                fi
                sudo systemctl reload nginx || error_exit "Failed to reload nginx."
            fi
        else
            if [[ "$FASTAPI_ACTIVE" == "false" ]]; then
                info "FastAPI service not running. Starting it..."
                sudo systemctl start pifitness-fastapi.service
            fi
            if [[ "$PM2_ACTIVE" == "false" ]]; then
                info "Next.js not running. Starting it..."
                cd "$FRONTEND_DIR"
                unset NEXT_PUBLIC_API_URL
                export NEXT_PUBLIC_APP_ENV=production
                # Clear any old PM2 state before starting
                rm -f ~/.pm2/dump.pm2
                pm2 start npm --name pifitness-next -- run start -- --port 3000
                pm2 save --force
                cd "$PROJECT_DIR"
            fi
            NGINX_TEMPLATE="$PROJECT_DIR/deployment/nginx-react.conf"
            NGINX_SITE="/etc/nginx/sites-available/pifitness"
            if [[ -f "$NGINX_TEMPLATE" ]]; then
                sudo cp "$NGINX_TEMPLATE" "$NGINX_SITE" 2>/dev/null || true
                sudo nginx -t 2>/dev/null && sudo systemctl reload nginx 2>/dev/null || true
            fi
            info "Services verified and started."
        fi
        exit 0
    fi

    info "Changed files:"
    echo "$CHANGED_FILES"

    git reset --hard origin/"$TARGET"

    find "$PROJECT_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find "$PROJECT_DIR" -type f -name "*.pyc" -exec rm -f {} + 2>/dev/null || true

    ONLY_CONFIG=true
    BACKEND_CHANGED=false
    FRONTEND_CHANGED=false
    for f in $CHANGED_FILES; do
        case "$f" in
            deployment/*|scripts/*|tests/*|README*|.gitignore|*.md)
                ;;
            backend/*|backend_functions/*)
                BACKEND_CHANGED=true
                ONLY_CONFIG=false
                ;;
            frontend/*)
                FRONTEND_CHANGED=true
                ONLY_CONFIG=false
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

    cp /home/god/Documents/.env "$PROJECT_DIR/backend/.env" 2>/dev/null || true
    unset NEXT_PUBLIC_API_URL
    export NEXT_PUBLIC_APP_ENV=production

    if [[ "$BACKEND_CHANGED" == "true" && "$FRONTEND_CHANGED" == "false" ]]; then
        info "Backend-only changes detected. Restarting FastAPI only..."
        sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
        sleep 1
        sudo systemctl start pifitness-fastapi.service
        info "FastAPI restarted."

    elif [[ "$FRONTEND_CHANGED" == "true" && "$BACKEND_CHANGED" == "false" ]]; then
        info "Frontend-only changes detected. Rebuilding and restarting Next.js..."
        cd "$FRONTEND_DIR"
        rm -rf .next
        npm run build
        pm2 delete pifitness-next 2>/dev/null || true
        rm -f ~/.pm2/dump.pm2
        pm2 start npm --name pifitness-next -- run start -- --port 3000
        pm2 save --force
        cd "$PROJECT_DIR"
        info "Next.js rebuilt and restarted."

    elif [[ "$BACKEND_CHANGED" == "true" && "$FRONTEND_CHANGED" == "true" ]]; then
        info "Both backend and frontend changed. Full restart..."
        sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
        cd "$FRONTEND_DIR"
        rm -rf .next
        npm run build
        pm2 delete pifitness-next 2>/dev/null || true
        rm -f ~/.pm2/dump.pm2
        pm2 start npm --name pifitness-next -- run start -- --port 3000
        pm2 save --force
        cd "$PROJECT_DIR"
        sleep 1
        sudo systemctl start pifitness-fastapi.service
        info "Both services restarted."
    fi

    sudo nginx -t 2>/dev/null && sudo systemctl reload nginx 2>/dev/null || true

    info "Fast deployment of react-ui completed."
    exit 0
fi

# ======================================================================
# FULL DEPLOY MODE (default)
# ======================================================================
info "=== FULL DEPLOY MODE ==="

# --- 1. Stop all services ---
info "Stopping any running services..."
rm -f /tmp/*.sock /tmp/*.pid 2>/dev/null || true

sudo systemctl stop pifitness_agent.service 2>/dev/null || true
sudo systemctl stop pifitness_agent.timer 2>/dev/null || true
sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
sudo systemctl stop pifitness-streamlit.service 2>/dev/null || true
pm2 delete pifitness-next 2>/dev/null || true
# Clear PM2 saved state
rm -f ~/.pm2/dump.pm2
sleep 2

pkill -9 -f "uvicorn" 2>/dev/null || true
pkill -9 -f "streamlit run" 2>/dev/null || true
pkill -9 -f "next" 2>/dev/null || true
for port in 8000 8501 3000; do
    lsof -ti :$port 2>/dev/null | xargs -r kill -9 2>/dev/null || true
done

# --- 2. Purge caches ---
info "Purging caches..."
find "$PROJECT_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$PROJECT_DIR" -type f -name "*.pyc" -exec rm -f {} + 2>/dev/null || true
rm -rf "$FRONTEND_DIR/.next" 2>/dev/null || true
sudo rm -rf /var/cache/nginx/* 2>/dev/null || true

# --- 3. Back up local config ---
info "Backing up local configuration..."
cd "$PROJECT_DIR"

[ -f backend/.env ] && cp backend/.env /tmp/.env.backup || true
[ -f .env ] && cp .env /tmp/.env.root.backup || true

for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
    [ -f "$PROJECT_DIR/$token_file" ] && cp "$PROJECT_DIR/$token_file" "/tmp/${token_file}.backup" || true
done

# --- 4. Pull the branch ---
info "Pulling branch: $TARGET"
git fetch origin
git reset --hard origin/"$TARGET"

# --- 5. Restore config ---
info "Restoring configuration..."
cp /home/god/Documents/.env "$PROJECT_DIR/backend/.env" 2>/dev/null || cp /tmp/.env.backup "$PROJECT_DIR/backend/.env" 2>/dev/null || true
cp /tmp/.env.root.backup "$PROJECT_DIR/.env" 2>/dev/null || true
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

# --- 7. Install frontend dependencies ---
if [[ "$INSTALL_PACKAGES" == "true" ]]; then
    info "Installing npm packages..."
    cd "$FRONTEND_DIR"
    if [[ -f "$PROJECT_DIR/deployment/$NPM_REQUIREMENTS_FILE" ]]; then
        info "Installing npm packages from requirements file..."
        while IFS= read -r package || [[ -n "$package" ]]; do
            if [[ -n "$package" && "$package" != \#* ]]; then
                info "Installing/updating package: $package"
                npm install --no-save "$package" 2>/dev/null || warn "Failed to install $package"
            fi
        done < "$PROJECT_DIR/deployment/$NPM_REQUIREMENTS_FILE"
    else
        warn "npm_requirements.txt not found, running standard npm install..."
        npm install --no-save 2>/dev/null || warn "npm install had issues"
    fi
    cd "$PROJECT_DIR"
else
    info "Skipping npm package checks"
fi

# --- 8. Run tests ---
info "Running automated tests..."
if ! pytest "$PROJECT_DIR/tests/" -v; then
    error_exit "Tests failed, aborting deployment"
fi
info "All tests passed."

# --- 9. Install systemd service files ---
info "Installing systemd service files..."
sudo cp "$PROJECT_DIR/deployment/pifitness-fastapi.service" /etc/systemd/system/
sudo cp "$PROJECT_DIR/deployment/pifitness-streamlit.service" /etc/systemd/system/
sudo systemctl daemon-reload

# --- 10. Build and start Next.js ---
info "Setting up Next.js server..."
cd "$FRONTEND_DIR"

if ! command -v pm2 &> /dev/null; then
    info "Installing PM2 for process management..."
    sudo npm install -g pm2
fi

# Purge environment variables and any old PM2 state
unset NEXT_PUBLIC_API_URL
export NEXT_PUBLIC_APP_ENV=production
# Wipe PM2 saved state to guarantee no stale env vars
rm -rf ~/.pm2

info "Cleaning up previous Next.js build artifacts..."
rm -rf .next

info "Building Next.js application..."
npm run build

# Start Next.js with PM2 (fresh PM2 state)
pm2 start npm --name pifitness-next -- run start -- --port 3000
pm2 save --force

if ! pm2 show pifitness-next | grep -q "status.*online"; then
    error_exit "PM2 failed to start Next.js server. Check logs with: pm2 logs pifitness-next"
fi

cd "$PROJECT_DIR"

# --- 11. Start FastAPI service ---
info "Starting FastAPI service..."
sudo systemctl start pifitness-fastapi.service
TARGET_PORT=8000

# --- 12. Update nginx configuration ---
info "Updating nginx configuration..."
NGINX_TEMPLATE="$PROJECT_DIR/deployment/nginx-react.conf"
NGINX_SITE="/etc/nginx/sites-available/pifitness"

if [[ ! -f "$NGINX_TEMPLATE" ]]; then
    error_exit "Nginx template not found at $NGINX_TEMPLATE"
fi

sudo cp "$NGINX_TEMPLATE" "$NGINX_SITE"

if ! grep -q ":${TARGET_PORT};" "$NGINX_SITE"; then
    error_exit "Failed to update nginx configuration with port ${TARGET_PORT}"
fi

if [[ ! -f "/etc/nginx/sites-enabled/pifitness" ]]; then
    sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness
fi

sudo rm -f /etc/nginx/sites-enabled/streamlit 2>/dev/null || true

info "Testing nginx configuration..."
sudo nginx -t || error_exit "Nginx configuration test failed."

info "Reloading nginx..."
sudo systemctl reload nginx || error_exit "Failed to reload nginx."

info "Verifying nginx configuration..."
sudo nginx -T 2>/dev/null | grep -A5 "server_name pifitness.duckdns.org" | grep "proxy_pass" | grep -q ":${TARGET_PORT};" || warn "Nginx may not be using the expected port ${TARGET_PORT}"

# --- 13. Restart agent service ---
info "Restarting agent service..."
sudo systemctl start pifitness_agent.timer 2>/dev/null || warn "Agent timer not available"

# --- 14. Cleanup old backups (keep last 2) ---
cd /home/god/PiFitness/backups 2>/dev/null && ls -1t | tail -n +3 | xargs -r rm -rf 2>/dev/null || true

info "Deployment of react-ui completed successfully!"