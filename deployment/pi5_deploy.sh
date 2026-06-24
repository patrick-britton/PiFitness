#!/bin/bash
# PiFitness Deployment Script
# Usage: ./deployment/pi5_deploy.sh

set -e

# --- Colours for output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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
sudo systemctl stop pifitness-streamlit.service 2>/dev/null || true
sudo systemctl stop pifitness-fastapi.service 2>/dev/null || true
sudo pkill -f "streamlit run" 2>/dev/null || true

# --- 3. Backup current code (rollback point) ---
BACKUP_DIR="/home/god/PiFitness/backups/$(date +%Y%m%d_%H%M%S)_$BRANCH"
mkdir -p "$BACKUP_DIR"
cp -r /home/god/PiFitness/* "$BACKUP_DIR" 2>/dev/null || true
info "Backup saved to $BACKUP_DIR"

# --- 4. Pull the branch ---
cd /home/god/PiFitness
git fetch origin
git checkout "$BRANCH"
git pull origin "$BRANCH"

# --- 5. Install/update Python dependencies ---
source venv/bin/activate
pip install -r deployment/library_requirements.txt

# Install/update systemd service files
sudo cp /home/god/PiFitness/deployment/pifitness-streamlit.service /etc/systemd/system/
sudo cp /home/god/PiFitness/deployment/pifitness-fastapi.service /etc/systemd/system/
sudo systemctl daemon-reload

# --- 6. Branch-specific actions ---
if [[ "$BRANCH" == "react-ui" ]]; then
    info "Building React frontend..."
    cd frontend/pifitness
    npm install
    npm run build
    # Copy build output to backend/static (FastAPI will serve it)
    mkdir -p ../../backend/static
    cp -r out/* ../../backend/static/
    cd ../..
fi

# --- 7. Start the appropriate service ---
if [[ "$BRANCH" == "streamlit-prd" ]]; then
    info "Starting Streamlit service..."
    sudo systemctl start pifitness-streamlit.service
    TARGET_PORT=8501
else
    info "Starting FastAPI service..."
    sudo systemctl start pifitness-fastapi.service
    TARGET_PORT=8000
fi

# --- 8. Update nginx configuration ---
NGINX_SITE="/etc/nginx/sites-available/pifitness"
NGINX_TEMPLATE="/home/god/PiFitness/deployment/nginx-template.conf"

# Ensure template exists
if [[ ! -f "$NGINX_TEMPLATE" ]]; then
    error_exit "Nginx template not found at $NGINX_TEMPLATE"
fi

# Replace PORT placeholder with target port
info "Updating nginx configuration to use port ${TARGET_PORT}..."
sudo sed "s/PORT/${TARGET_PORT}/g" "$NGINX_TEMPLATE" | sudo tee "$NGINX_SITE" > /dev/null

# Verify the port was actually replaced
if ! grep -q ":${TARGET_PORT};" "$NGINX_SITE"; then
    error_exit "Failed to update nginx configuration with port ${TARGET_PORT}. Check if PORT placeholder exists in template."
fi

# Ensure symlink exists in sites-enabled
if [[ ! -f "/etc/nginx/sites-enabled/pifitness" ]]; then
    info "Creating nginx symlink..."
    sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness
fi

# Test and reload nginx
info "Testing nginx configuration..."
sudo nginx -t || error_exit "Nginx configuration test failed."

info "Reloading nginx..."
sudo systemctl reload nginx || error_exit "Failed to reload nginx."

# Verify nginx is using the correct port
info "Verifying nginx configuration..."
sudo nginx -T 2>/dev/null | grep -A5 "server_name pifitness.duckdns.org" | grep "proxy_pass" | grep -q ":${TARGET_PORT};" || warn "Nginx may not be using the expected port ${TARGET_PORT}"

info "Nginx configured to use port $TARGET_PORT."

# --- 9. Cleanup old backups (keep last 2) ---
cd /home/god/PiFitness/backups
ls -1t | tail -n +3 | xargs -r rm -rf

info "Deployment of $BRANCH completed successfully!"