#!/bin/bash
# PiFitness Deployment Script
# Usage: ./deploy/scripts/pi5_deploy.sh

set -e

# --- Colours for output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Systemd unit directory: repo-driven source of unit files.
# No unit name is hardcoded in this script — everything is discovered from
# the files in this directory. Units whose filename contains "-test" belong
# to the TEST environment; all other pifitness-* units belong to PRD.
SYSTEMD_DIR="/home/god/PiFitness/deploy/systemd"

# --- Helper functions ---
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

cleanup_processes() {
    info "Cleaning up temporary files..."
    sudo rm -f /tmp/*.sock 2>/dev/null || true
    sudo rm -f /tmp/*.pid 2>/dev/null || true
}

manage_agent_service() {
    info "Managing PRD services from systemd directory..."
    if [[ -d "$SYSTEMD_DIR" ]]; then
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        if sudo systemctl is-active --quiet "$install_name" 2>/dev/null; then
          info "Stopping $install_name..."
          sudo systemctl stop "$install_name" 2>/dev/null || true
        else
          info "$install_name not running"
        fi
      done
    fi
}

restart_agent_service() {
    info "Restarting PRD services from systemd directory..."
    sudo systemctl daemon-reload
    if [[ -d "$SYSTEMD_DIR" ]]; then
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        sudo systemctl start "$install_name" 2>/dev/null || true
      done
    fi

    if [[ -d "$SYSTEMD_DIR" ]]; then
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        if sudo systemctl is-active --quiet "$install_name" 2>/dev/null; then
          info "$install_name started successfully"
        else
          warn "Failed to start $install_name - check logs"
          sudo systemctl status "$install_name" --no-pager || true
        fi
      done
    fi
}

detect_and_kill_processes() {
    info "Detecting running processes..."

    # Stop PRD units from systemd directory (no hardcoded unit names)
    if [[ -d "$SYSTEMD_DIR" ]]; then
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        if sudo systemctl is-active --quiet "$install_name" 2>/dev/null; then
          info "Found running $install_name - stopping gracefully..."
          sudo systemctl stop "$install_name" 2>/dev/null || true
          sleep 2
          if sudo systemctl is-active --quiet "$install_name" 2>/dev/null; then
            warn "$install_name still running - force stopping..."
            sudo systemctl stop "$install_name" 2>/dev/null || true
          else
            info "$install_name stopped successfully"
          fi
        fi
      done
    fi

    info "Checking for processes using deployment ports..."
    for port in 8000 8501 3000; do
        if sudo lsof -ti :$port > /dev/null; then
            warn "Process found using port $port - killing..."
            sudo lsof -ti :$port | xargs -r sudo kill -9 || warn "Failed to kill processes on port $port"
        fi
    done
}

# --- 1. Ask for branch ---
echo "Which branch to deploy?"
echo "  1) streamlit-prd (legacy Streamlit)"
echo "  2) react-ui (new FastAPI + React)"
read -rp "Enter 1 or 2: " branch_choice

case $branch_choice in
    1) BRANCH="streamlit-prd" ;;
    2) BRANCH="react-ui" ;;
    *) error_exit "Invalid choice. Exiting." ;;
esac

info "Deploying branch: $BRANCH"

# --- 2. Stop all services ---
info "Stopping any running services..."
cleanup_processes
manage_agent_service
detect_and_kill_processes

# --- 3. Backup current code (rollback point) ---
echo "Backup Turned Off"

# --- 4. Pull the branch ---
cd /home/god/PiFitness

info "Protecting local configuration files..."
[ -f backend/.env ] && cp backend/.env /tmp/.env.backup || true
[ -f .env ] && cp .env /tmp/.env.root.backup || true

info "Resetting to remote branch (receiver mode)..."
git fetch origin
git reset --hard origin/"$BRANCH"

[ -f /tmp/.env.backup ] && cp /tmp/.env.backup backend/.env && info "Restored backend/.env"
[ -f /tmp/.env.root.backup ] && cp /tmp/.env.root.backup .env && info "Restored .env"

# --- 5. Install/update Python dependencies ---
source venv/bin/activate

echo "Do you want to perform package dependency checks?"
echo "  1) Yes, install/update all Python and npm packages (recommended)"
echo "  2) No, skip package checks (faster, but may miss updates)"
read -rp "Enter 1 or 2: " package_choice

if [[ "$package_choice" == "1" ]]; then
    info "Installing/updating Python dependencies..."
    pip install -r deploy/scripts/library_requirements.txt
    
    # Garmin integration lives in backend/pifitness/integrations/garmin/
    # and removed as an editable dependency in INT-10b. No install step needed.
    
    # Install Playwright browser for Garmin browser-based login
    info "Installing Playwright Chromium browser..."
    python -m playwright install chromium
else
    info "Skipping Python package checks"
fi

# DEFENSIVE PURGE: Clear Python Bytecode
info "Purging Python bytecode caches..."
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -exec rm -f {} +

# --- 5.5. Run automated tests (React UI branch only) ---
if [[ "$BRANCH" == "react-ui" ]]; then
    info "Running automated tests..."
    if ! pytest tests/ -v; then
        error_exit "Tests failed, aborting deployment"
    fi
    info "All tests passed successfully"
fi

# --- 5.7. Install frontend dependencies (React UI branch only) ---
if [[ "$BRANCH" == "react-ui" && "$package_choice" == "1" ]]; then
    info "Installing frontend dependencies..."
    cd frontend/pifitness
    if [[ -f "../../deploy/scripts/npm_requirements.txt" ]]; then
        info "Installing npm packages from requirements file..."
        while IFS= read -r package || [[ -n "$package" ]]; do
            if [[ -n "$package" && "$package" != \#* ]]; then
                info "Installing/updating package: $package"
                npm install --no-save "$package"
            fi
        done < ../../deploy/scripts/npm_requirements.txt
    else
        warn "npm_requirements.txt not found, running standard npm install..."
        npm install --no-save
    fi
    cd /home/god/PiFitness || error_exit "Failed to return to project root after npm install"
fi

# Install/update systemd service files from systemd directory (no hardcoded unit names)
if [[ -d "$SYSTEMD_DIR" ]]; then
  for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    info "Installing $install_name..."
    sudo cp "$unit_file" /etc/systemd/system/"$install_name"
  done
  sudo systemctl daemon-reload
fi

# --- 6. Branch-specific actions ---
if [[ "$BRANCH" == "react-ui" ]]; then
    info "Setting up Next.js server (replacing static export)..."
    cd frontend/pifitness || error_exit "frontend/pifitness not found. Verify directory structure."

    if ! command -v pm2 &> /dev/null; then
        info "Installing PM2 for process management..."
        sudo npm install -g pm2
    fi

    # CRITICAL DEFENSIVE FIX: Eradicate the localhost variable from the environment
    info "Purging environment of local API URLs..."
    unset NEXT_PUBLIC_API_URL
    export NEXT_PUBLIC_APP_ENV=production

    info "Cleaning up previous Next.js build artifacts..."
    rm -rf .next
    pm2 delete pifitness-next 2>/dev/null || true

    info "Building Next.js application..."
    npm run build

    # Start Next.js with PM2 and force the save state
    pm2 start npm --name pifitness-next -- run start -- --port 3000
    pm2 save --force

    if ! pm2 show pifitness-next | grep -q "status.*online"; then
        error_exit "PM2 failed to start Next.js server. Check logs with: pm2 logs pifitness-next"
    fi

    cd /home/god/PiFitness
fi

# --- 7. Start the appropriate service ---
if [[ "$BRANCH" == "streamlit-prd" ]]; then
    info "Starting PRD services from systemd directory..."
    if [[ -d "$SYSTEMD_DIR" ]]; then
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        sudo systemctl start "$install_name" 2>/dev/null || true
      done
    fi
    TARGET_PORT=8501
else
    info "Starting PRD services from systemd directory..."
    if [[ -d "$SYSTEMD_DIR" ]]; then
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        sudo systemctl start "$install_name" 2>/dev/null || true
      done
    fi
    TARGET_PORT=8000
fi

# --- 8. Update nginx configuration ---
NGINX_SITE="/etc/nginx/sites-available/pifitness"
NGINX_TEMPLATE="/home/god/PiFitness/deploy/nginx/nginx-template.conf"

if [[ ! -f "$NGINX_TEMPLATE" ]]; then
    error_exit "Nginx template not found at $NGINX_TEMPLATE"
fi

info "Updating nginx configuration to use port ${TARGET_PORT}..."
sudo sed "s/FASTAPI_PORT/${TARGET_PORT}/g" "$NGINX_TEMPLATE" | sudo tee "$NGINX_SITE" > /dev/null

if ! grep -q ":${TARGET_PORT};" "$NGINX_SITE"; then
    error_exit "Failed to update nginx configuration with port ${TARGET_PORT}."
fi

if [[ ! -f "/etc/nginx/sites-enabled/pifitness" ]]; then
    info "Creating nginx symlink..."
    sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness
fi

if [[ "$BRANCH" == "react-ui" ]]; then
    if [[ -L "/etc/nginx/sites-enabled/streamlit" ]]; then
        info "Removing stale streamlit nginx symlink..."
        sudo rm -f "/etc/nginx/sites-enabled/streamlit"
    fi
elif [[ "$BRANCH" == "streamlit-prd" ]]; then
    if [[ -L "/etc/nginx/sites-enabled/pifitness" ]]; then
        info "Removing stale pifitness nginx symlink..."
        sudo rm -f "/etc/nginx/sites-enabled/pifitness"
    fi
fi

# DEFENSIVE PURGE: Clear Nginx Proxy Caches
info "Clearing Nginx caches..."
sudo rm -rf /var/cache/nginx/* 2>/dev/null || true

info "Testing nginx configuration..."
sudo nginx -t || error_exit "Nginx configuration test failed."

info "Reloading nginx..."
sudo systemctl reload nginx || error_exit "Failed to reload nginx."

info "Verifying nginx configuration..."
sudo nginx -T 2>/dev/null | grep -A5 "server_name pifitness.duckdns.org" | grep "proxy_pass" | grep -q ":${TARGET_PORT};" || warn "Nginx may not be using the expected port ${TARGET_PORT}"

# --- 9. Restart agent service ---
restart_agent_service

# --- 10. Cleanup old backups (keep last 2) ---
cd /home/god/PiFitness/backups 2>/dev/null || true
ls -1t | tail -n +3 | xargs -r rm -rf

info "Deployment of $BRANCH completed successfully!"