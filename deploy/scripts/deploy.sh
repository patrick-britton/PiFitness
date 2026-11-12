#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PiFitness React Deployment Script
# Called by bootstrap.sh -- target: react-ui
#
# Usage (called from bootstrap.sh):
#   deploy_react.sh --install-packages=true/false --fast=true/false --nuclear=true/false
#
# Modes:
#   nuclear -- wipe the project directory, re-clone, rebuild from scratch
#   fast    -- pull, restart only what changed, skip tests and package installs
#   full    -- pull, install packages, rebuild, restart (default)

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
REQUIREMENTS_FILE="library_requirements.txt"
NPM_REQUIREMENTS_FILE="npm_requirements.txt"
DBMATE_BIN="/usr/local/bin/dbmate"
# Systemd unit directory: repo-driven source of unit files.
# No unit name is hardcoded in this script — everything is discovered from
# the files in this directory. Units whose filename contains "-test" belong
# to the TEST environment; all other pifitness-* units belong to PRD.
SYSTEMD_DIR="$PROJECT_DIR/deploy/systemd"

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

    info "Backing up .env..."
    [ -f "$PROJECT_DIR/backend/.env" ] && cp "$PROJECT_DIR/backend/.env" /home/god/Documents/.env.pre-nuclear
    cp /home/god/Documents/.env /home/god/Documents/.env.pre-nuclear.master 2>/dev/null || true

    info "Backing up auth tokens..."
    for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
        [ -f "$PROJECT_DIR/$token_file" ] && cp "$PROJECT_DIR/$token_file" "/tmp/${token_file}.backup" || true
    done

    info "Stopping any running services before wipe..."
    if [[ -d "$SYSTEMD_DIR" ]]; then
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        sudo systemctl stop "$install_name" 2>/dev/null || true
      done
    fi
    pm2 delete pifitness-next 2>/dev/null || true
    rm -f ~/.pm2/dump.pm2
    for port in 8000 3000; do
        lsof -ti :$port 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    done

    info "Wiping project directory, venv, and npm caches..."
    rm -rf "$PROJECT_DIR"
    rm -rf "$VENV_DIR"
    rm -rf ~/.npm/_cacache
    rm -rf ~/.pm2

    info "Cloning repository..."
    git clone https://github.com/patrick-britton/PiFitness.git "$PROJECT_DIR"
    cd "$PROJECT_DIR"
    git fetch origin
    git checkout "$TARGET"

    cp /home/god/Documents/.env "$PROJECT_DIR/backend/.env"
    cp /home/god/Documents/.env "$PROJECT_DIR/.env"

    for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
        [ -f "/tmp/${token_file}.backup" ] && cp "/tmp/${token_file}.backup" "$PROJECT_DIR/$token_file" || true
    done

    info "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"

    info "Installing Python dependencies..."
    pip install -r "$PROJECT_DIR/deploy/scripts/$REQUIREMENTS_FILE"

    source "$SCRIPT_DIR/lib/ensure_binary.sh"
    ensure_binary "$DBMATE_BIN" "2.36.0" \
        "https://github.com/amacneil/dbmate/releases/download/v2.36.0/dbmate-linux-arm64" "dbmate"

    info "Installing npm packages..."
    cd "$FRONTEND_DIR"
    npm install --no-save 2>/dev/null || true
    if [[ -f "$PROJECT_DIR/deploy/scripts/$NPM_REQUIREMENTS_FILE" ]]; then
        while IFS= read -r package || [[ -n "$package" ]]; do
            if [[ -n "$package" && "$package" != \#* ]]; then
                npm install --no-save "$package" 2>/dev/null || true
            fi
        done < "$PROJECT_DIR/deploy/scripts/$NPM_REQUIREMENTS_FILE"
    fi

    info "Building Next.js application..."
    npm run build

    info "Installing systemd units from systemd directory..."
    if [[ -d "$SYSTEMD_DIR" ]]; then
      # Install all unit files from the repo's systemd directory
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        info "  installing $install_name..."
        sudo cp "$unit_file" /etc/systemd/system/"$install_name"
      done
      sudo systemctl daemon-reload

      # Enable and start PRD units (filename does not contain "-test")
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        sudo systemctl enable "$install_name" 2>/dev/null || true
        sudo systemctl start "$install_name" 2>/dev/null || true
      done

      # Build list of expected unit names for stale cleanup
      expected_units=""
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        expected_units="${expected_units} $(basename "$unit_file") "
      done

      # Remove installed pifitness-* units not in the systemd directory (stale cleanup)
      while read -r installed_unit _rest; do
        [[ "$installed_unit" == \#* || -z "$installed_unit" ]] && continue
        [[ "$installed_unit" == pifitness-* ]] || continue
        case " $expected_units " in
          *" $installed_unit "*) continue ;;
        esac
        info "  removing stale unit $installed_unit..."
        sudo systemctl disable "$installed_unit" 2>/dev/null || true
        sudo systemctl stop "$installed_unit" 2>/dev/null || true
        sudo rm -f /etc/systemd/system/"$installed_unit"
      done < <(systemctl list-unit-files --type=service,timer --no-pager --no-legend 2>/dev/null)
      sudo systemctl daemon-reload
    fi

    if ! command -v pm2 &> /dev/null; then
        sudo npm install -g pm2
    fi
    if ! sudo systemctl is-enabled --quiet pm2-god.service 2>/dev/null; then
        info "Configuring PM2 to survive reboot..."
        if ! sudo env PATH=$PATH:/usr/bin pm2 startup systemd -u god --hp /home/god > /dev/null 2>&1; then
            warn "pm2 startup failed — PM2 will not survive reboot"
        fi
    fi
    pm2 kill 2>/dev/null || true
    rm -rf ~/.pm2
    unset NEXT_PUBLIC_API_URL
    export NEXT_PUBLIC_APP_ENV=production
    pm2 start npm --name pifitness-next -- run start -- --port 3000 --hostname 127.0.0.1
    pm2 save --force

    NGINX_TEMPLATE="$PROJECT_DIR/deploy/nginx/nginx-react.conf"
    NGINX_SITE="/etc/nginx/sites-available/pifitness"
    sudo cp "$NGINX_TEMPLATE" "$NGINX_SITE"
    if [[ ! -f "/etc/nginx/sites-enabled/pifitness" ]]; then
        sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness
    fi
    if ! sudo nginx -t; then
        error_exit "Nginx configuration test failed!"
    fi
    sudo systemctl reload nginx || error_exit "Failed to reload nginx."

    info "Nuclear deployment of react-ui completed."
    exit 0
fi

# ======================================================================
# FAST MODE: git pull + selective restart
# ======================================================================
if [[ "$FAST" == "true" ]]; then
    info "=== FAST MODE ==="

    if ! sudo systemctl is-enabled --quiet pm2-god.service 2>/dev/null; then
        info "Configuring PM2 to survive reboot..."
        if ! sudo env PATH=$PATH:/usr/bin pm2 startup systemd -u god --hp /home/god > /dev/null 2>&1; then
            warn "pm2 startup failed — PM2 will not survive reboot"
        fi
    fi

    # Enable PRD units from systemd directory (no hardcoded unit names)
    if [[ -d "$SYSTEMD_DIR" ]]; then
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        sudo systemctl enable "$install_name" 2>/dev/null || true
      done
    fi

    cd "$PROJECT_DIR"

    git fetch origin

    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$CURRENT_BRANCH" != "$TARGET" ]]; then
        info "Switching from '$CURRENT_BRANCH' to '$TARGET'..."
        git checkout --force "$TARGET"
    fi

    if [[ "$CURRENT_BRANCH" != "$TARGET" ]]; then
        info "Switching branches. Forcing full restart & nginx realignment..."

        cp /home/god/Documents/.env "$PROJECT_DIR/backend/.env" 2>/dev/null || true
        cp /home/god/Documents/.env "$PROJECT_DIR/.env"        2>/dev/null || true

        # Stop PRD units from systemd directory (no hardcoded unit names)
        if [[ -d "$SYSTEMD_DIR" ]]; then
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            [[ "$install_name" == *-test.* ]] && continue
            sudo systemctl stop "$install_name" 2>/dev/null || true
          done
        fi

        pm2 delete pifitness-next 2>/dev/null || true
        rm -f ~/.pm2/dump.pm2
        cd "$FRONTEND_DIR"
        unset NEXT_PUBLIC_API_URL
        export NEXT_PUBLIC_APP_ENV=production
        pm2 start npm --name pifitness-next -- run start -- --port 3000 --hostname 127.0.0.1
        pm2 save --force
        cd "$PROJECT_DIR"

        NGINX_TEMPLATE="$PROJECT_DIR/deploy/nginx/nginx-react.conf"
        NGINX_SITE="/etc/nginx/sites-available/pifitness"
        if [[ -f "$NGINX_TEMPLATE" ]]; then
            sudo cp "$NGINX_TEMPLATE" "$NGINX_SITE"
            sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness 2>/dev/null || true
            if ! sudo nginx -t; then
                error_exit "Nginx configuration test failed!"
            fi
            sudo systemctl reload nginx || error_exit "Failed to reload nginx."
        fi

        # Install systemd units from systemd directory (no hardcoded unit names)
        if [[ -d "$SYSTEMD_DIR" ]]; then
          # Install all unit files from the repo's systemd directory
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            info "  installing $install_name..."
            sudo cp "$unit_file" /etc/systemd/system/"$install_name"
          done
          sudo systemctl daemon-reload

          # Enable and start PRD units (filename does not contain "-test")
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            [[ "$install_name" == *-test.* ]] && continue
            sudo systemctl enable "$install_name" 2>/dev/null || true
            sudo systemctl start "$install_name" 2>/dev/null || true
          done

          # Build list of expected unit names for stale cleanup
          expected_units=""
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            expected_units="${expected_units} $(basename "$unit_file") "
          done

          # Remove installed pifitness-* units not in the systemd directory (stale cleanup)
          while read -r installed_unit _rest; do
            [[ "$installed_unit" == \#* || -z "$installed_unit" ]] && continue
            [[ "$installed_unit" == pifitness-* ]] || continue
            case " $expected_units " in
              *" $installed_unit "*) continue ;;
            esac
            info "  removing stale unit $installed_unit..."
            sudo systemctl disable "$installed_unit" 2>/dev/null || true
            sudo systemctl stop "$installed_unit" 2>/dev/null || true
            sudo rm -f /etc/systemd/system/"$installed_unit"
          done < <(systemctl list-unit-files --type=service,timer --no-pager --no-legend 2>/dev/null)
          sudo systemctl daemon-reload
        fi

        info "Services and nginx realigned for react-ui."
        exit 0
    fi

    CHANGED_FILES=$(git diff HEAD..origin/"$TARGET" --name-only)

    if [[ -z "$CHANGED_FILES" ]]; then
        info "No code changes detected. Verifying services are running..."

        # Check if any PRD service from systemd directory is active (no hardcoded names)
        PRD_ANY_ACTIVE=false
        if [[ -d "$SYSTEMD_DIR" ]]; then
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            [[ "$install_name" == *-test.* ]] && continue
            if sudo systemctl is-active --quiet "$install_name" 2>/dev/null; then
              PRD_ANY_ACTIVE=true
              break
            fi
          done
        fi

        PM2_ACTIVE=false
        if pm2 show pifitness-next 2>/dev/null | grep -q "status.*online"; then
            PM2_ACTIVE=true
        fi

        if [[ "$PRD_ANY_ACTIVE" == "true" && "$PM2_ACTIVE" == "true" ]]; then
            info "PRD services and Next.js already running."
            NGINX_SITE="/etc/nginx/sites-available/pifitness"
            if [[ -f "$NGINX_SITE" ]] && ! grep -q ":8000;" "$NGINX_SITE"; then
                info "Correcting Nginx routing to port 8000..."
                NGINX_TEMPLATE="$PROJECT_DIR/deploy/nginx/nginx-react.conf"
                sudo cp "$NGINX_TEMPLATE" "$NGINX_SITE"
                if ! sudo nginx -t; then
                    error_exit "Nginx configuration test failed!"
                fi
                sudo systemctl reload nginx || error_exit "Failed to reload nginx."
            fi
        else
            if [[ "$PRD_ANY_ACTIVE" == "false" ]]; then
                info "PRD services not running. Starting them from systemd directory..."
                if [[ -d "$SYSTEMD_DIR" ]]; then
                  for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
                    [[ -f "$unit_file" ]] || continue
                    install_name="$(basename "$unit_file")"
                    [[ "$install_name" == *-test.* ]] && continue
                    sudo systemctl start "$install_name" 2>/dev/null || true
                  done
                fi
            fi
            if [[ "$PM2_ACTIVE" == "false" ]]; then
                info "Next.js not running. Starting it..."
                cd "$FRONTEND_DIR"
                unset NEXT_PUBLIC_API_URL
                export NEXT_PUBLIC_APP_ENV=production
                rm -f ~/.pm2/dump.pm2
                pm2 start npm --name pifitness-next -- run start -- --port 3000 --hostname 127.0.0.1
                pm2 save --force
                cd "$PROJECT_DIR"
            fi
            NGINX_TEMPLATE="$PROJECT_DIR/deploy/nginx/nginx-react.conf"
            NGINX_SITE="/etc/nginx/sites-available/pifitness"
            if [[ -f "$NGINX_TEMPLATE" ]]; then
                sudo cp "$NGINX_TEMPLATE" "$NGINX_SITE" 2>/dev/null || true
                sudo nginx -t 2>/dev/null && sudo systemctl reload nginx 2>/dev/null || true
            fi
            info "Services verified and started."
        fi

        # Install systemd units from systemd directory (no hardcoded unit names)
        if [[ -d "$SYSTEMD_DIR" ]]; then
          # Install all unit files from the repo's systemd directory
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            info "  installing $install_name..."
            sudo cp "$unit_file" /etc/systemd/system/"$install_name"
          done
          sudo systemctl daemon-reload

          # Enable and start PRD units (filename does not contain "-test")
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            [[ "$install_name" == *-test.* ]] && continue
            sudo systemctl enable "$install_name" 2>/dev/null || true
            sudo systemctl start "$install_name" 2>/dev/null || true
          done

          # Build list of expected unit names for stale cleanup
          expected_units=""
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            expected_units="${expected_units} $(basename "$unit_file") "
          done

          # Remove installed pifitness-* units not in the systemd directory (stale cleanup)
          while read -r installed_unit _rest; do
            [[ "$installed_unit" == \#* || -z "$installed_unit" ]] && continue
            [[ "$installed_unit" == pifitness-* ]] || continue
            case " $expected_units " in
              *" $installed_unit "*) continue ;;
            esac
            info "  removing stale unit $installed_unit..."
            sudo systemctl disable "$installed_unit" 2>/dev/null || true
            sudo systemctl stop "$installed_unit" 2>/dev/null || true
            sudo rm -f /etc/systemd/system/"$installed_unit"
          done < <(systemctl list-unit-files --type=service,timer --no-pager --no-legend 2>/dev/null)
          sudo systemctl daemon-reload
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
            deploy/*|scripts/*|tests/*|README*|.gitignore|*.md)
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

        # Install systemd units from systemd directory (no hardcoded unit names)
        if [[ -d "$SYSTEMD_DIR" ]]; then
          # Install all unit files from the repo's systemd directory
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            info "  installing $install_name..."
            sudo cp "$unit_file" /etc/systemd/system/"$install_name"
          done
          sudo systemctl daemon-reload

          # Enable and start PRD units (filename does not contain "-test")
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            [[ "$install_name" == *-test.* ]] && continue
            sudo systemctl enable "$install_name" 2>/dev/null || true
            sudo systemctl start "$install_name" 2>/dev/null || true
          done

          # Build list of expected unit names for stale cleanup
          expected_units=""
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            expected_units="${expected_units} $(basename "$unit_file") "
          done

          # Remove installed pifitness-* units not in the systemd directory (stale cleanup)
          while read -r installed_unit _rest; do
            [[ "$installed_unit" == \#* || -z "$installed_unit" ]] && continue
            [[ "$installed_unit" == pifitness-* ]] || continue
            case " $expected_units " in
              *" $installed_unit "*) continue ;;
            esac
            info "  removing stale unit $installed_unit..."
            sudo systemctl disable "$installed_unit" 2>/dev/null || true
            sudo systemctl stop "$installed_unit" 2>/dev/null || true
            sudo rm -f /etc/systemd/system/"$installed_unit"
          done < <(systemctl list-unit-files --type=service,timer --no-pager --no-legend 2>/dev/null)
          sudo systemctl daemon-reload
        fi

        exit 0
    fi

    cp /home/god/Documents/.env "$PROJECT_DIR/backend/.env" 2>/dev/null || true
    cp /home/god/Documents/.env "$PROJECT_DIR/.env"        2>/dev/null || true
    unset NEXT_PUBLIC_API_URL
    export NEXT_PUBLIC_APP_ENV=production

    if [[ "$BACKEND_CHANGED" == "true" && "$FRONTEND_CHANGED" == "false" ]]; then
        info "Backend-only changes detected. Restarting PRD services from systemd directory..."
        if [[ -d "$SYSTEMD_DIR" ]]; then
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            [[ "$install_name" == *-test.* ]] && continue
            sudo systemctl restart "$install_name" 2>/dev/null || true
          done
        fi
        info "PRD services restarted."

    elif [[ "$FRONTEND_CHANGED" == "true" && "$BACKEND_CHANGED" == "false" ]]; then
        info "Frontend-only changes detected. Rebuilding and restarting Next.js..."
        cd "$FRONTEND_DIR"
        rm -rf .next
        npm run build
        pm2 delete pifitness-next 2>/dev/null || true
        rm -f ~/.pm2/dump.pm2
        pm2 start npm --name pifitness-next -- run start -- --port 3000 --hostname 127.0.0.1
        pm2 save --force
        cd "$PROJECT_DIR"
        info "Next.js rebuilt and restarted."

    elif [[ "$BACKEND_CHANGED" == "true" && "$FRONTEND_CHANGED" == "true" ]]; then
        info "Both backend and frontend changed. Full restart..."
        if [[ -d "$SYSTEMD_DIR" ]]; then
          for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
            [[ -f "$unit_file" ]] || continue
            install_name="$(basename "$unit_file")"
            [[ "$install_name" == *-test.* ]] && continue
            sudo systemctl restart "$install_name" 2>/dev/null || true
          done
        fi
        cd "$FRONTEND_DIR"
        rm -rf .next
        npm run build
        pm2 delete pifitness-next 2>/dev/null || true
        rm -f ~/.pm2/dump.pm2
        pm2 start npm --name pifitness-next -- run start -- --port 3000 --hostname 127.0.0.1
        pm2 save --force
        cd "$PROJECT_DIR"
        info "Both services restarted."
    fi

    sudo nginx -t 2>/dev/null && sudo systemctl reload nginx 2>/dev/null || true

    # Install systemd units from systemd directory (no hardcoded unit names)
    if [[ -d "$SYSTEMD_DIR" ]]; then
      # Install all unit files from the repo's systemd directory
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        info "  installing $install_name..."
        sudo cp "$unit_file" /etc/systemd/system/"$install_name"
      done
      sudo systemctl daemon-reload

      # Enable and start PRD units (filename does not contain "-test")
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        install_name="$(basename "$unit_file")"
        [[ "$install_name" == *-test.* ]] && continue
        sudo systemctl enable "$install_name" 2>/dev/null || true
        sudo systemctl start "$install_name" 2>/dev/null || true
      done

      # Build list of expected unit names for stale cleanup
      expected_units=""
      for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
        [[ -f "$unit_file" ]] || continue
        expected_units="${expected_units} $(basename "$unit_file") "
      done

      # Remove installed pifitness-* units not in the systemd directory (stale cleanup)
      while read -r installed_unit _rest; do
        [[ "$installed_unit" == \#* || -z "$installed_unit" ]] && continue
        [[ "$installed_unit" == pifitness-* ]] || continue
        case " $expected_units " in
          *" $installed_unit "*) continue ;;
        esac
        info "  removing stale unit $installed_unit..."
        sudo systemctl disable "$installed_unit" 2>/dev/null || true
        sudo systemctl stop "$installed_unit" 2>/dev/null || true
        sudo rm -f /etc/systemd/system/"$installed_unit"
      done < <(systemctl list-unit-files --type=service,timer --no-pager --no-legend 2>/dev/null)
      sudo systemctl daemon-reload
    fi

    info "Fast deployment of react-ui completed."
    exit 0
fi

# ======================================================================
# FULL DEPLOY MODE (default)
# ======================================================================
info "=== FULL DEPLOY MODE ==="

info "Stopping any running services..."
rm -f /tmp/*.sock /tmp/*.pid 2>/dev/null || true

# Stop PRD units from systemd directory (no hardcoded unit names)
if [[ -d "$SYSTEMD_DIR" ]]; then
  for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    [[ "$install_name" == *-test.* ]] && continue
    sudo systemctl stop "$install_name" 2>/dev/null || true
  done
fi
pm2 delete pifitness-next 2>/dev/null || true
rm -f ~/.pm2/dump.pm2
sleep 2

pkill -9 -f "uvicorn" 2>/dev/null || true
pkill -9 -f "next"    2>/dev/null || true
for port in 8000 3000; do
    lsof -ti :$port 2>/dev/null | xargs -r kill -9 2>/dev/null || true
done

info "Purging caches..."
find "$PROJECT_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$PROJECT_DIR" -type f -name "*.pyc" -exec rm -f {} + 2>/dev/null || true
rm -rf "$FRONTEND_DIR/.next" 2>/dev/null || true
sudo rm -rf /var/cache/nginx/* 2>/dev/null || true

info "Backing up local configuration..."
cd "$PROJECT_DIR"

[ -f backend/.env ] && cp backend/.env /tmp/.env.backup || true
[ -f .env ]         && cp .env         /tmp/.env.root.backup || true

for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
    [ -f "$PROJECT_DIR/$token_file" ] && cp "$PROJECT_DIR/$token_file" "/tmp/${token_file}.backup" || true
done

info "Pulling branch: $TARGET"
git fetch origin
git reset --hard origin/"$TARGET"

info "Restoring configuration..."
cp /home/god/Documents/.env "$PROJECT_DIR/backend/.env" 2>/dev/null || cp /tmp/.env.backup "$PROJECT_DIR/backend/.env" 2>/dev/null || true
cp /home/god/Documents/.env "$PROJECT_DIR/.env"        2>/dev/null || cp /tmp/.env.root.backup "$PROJECT_DIR/.env" 2>/dev/null || true
for token_file in garmin_tokens.json oauth1_token.json .spotify_cache; do
    [ -f "/tmp/${token_file}.backup" ] && cp "/tmp/${token_file}.backup" "$PROJECT_DIR/$token_file" || true
done

source "$VENV_DIR/bin/activate"

if [[ "$INSTALL_PACKAGES" == "true" ]]; then
    info "Installing/updating Python dependencies..."
    pip install -r "$PROJECT_DIR/deploy/scripts/$REQUIREMENTS_FILE"
else
    info "Skipping Python package checks"
fi

if [[ "$INSTALL_PACKAGES" == "true" ]]; then
    info "Installing npm packages..."
    cd "$FRONTEND_DIR"
    if [[ -f "$PROJECT_DIR/deploy/scripts/$NPM_REQUIREMENTS_FILE" ]]; then
        info "Installing npm packages from requirements file..."
        while IFS= read -r package || [[ -n "$package" ]]; do
            if [[ -n "$package" && "$package" != \#* ]]; then
                info "Installing/updating package: $package"
                npm install --no-save "$package" 2>/dev/null || warn "Failed to install $package"
            fi
        done < "$PROJECT_DIR/deploy/scripts/$NPM_REQUIREMENTS_FILE"
    else
        warn "npm_requirements.txt not found, running standard npm install..."
        npm install --no-save 2>/dev/null || warn "npm install had issues"
    fi
    cd "$PROJECT_DIR"
else
    info "Skipping npm package checks"
fi

info "Skipping automated tests (D13)."

# Install systemd units from systemd directory (no hardcoded unit names)
if [[ -d "$SYSTEMD_DIR" ]]; then
  # Install all unit files from the repo's systemd directory
  for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    info "  installing $install_name..."
    sudo cp "$unit_file" /etc/systemd/system/"$install_name"
  done
  sudo systemctl daemon-reload

  # Enable and start PRD units (filename does not contain "-test")
  for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    [[ "$install_name" == *-test.* ]] && continue
    sudo systemctl enable "$install_name" 2>/dev/null || true
    sudo systemctl start "$install_name" 2>/dev/null || true
  done

  # Build list of expected unit names for stale cleanup
  expected_units=""
  for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
    [[ -f "$unit_file" ]] || continue
    expected_units="${expected_units} $(basename "$unit_file") "
  done

  # Remove installed pifitness-* units not in the systemd directory (stale cleanup)
  while read -r installed_unit _rest; do
    [[ "$installed_unit" == \#* || -z "$installed_unit" ]] && continue
    [[ "$installed_unit" == pifitness-* ]] || continue
    case " $expected_units " in
      *" $installed_unit "*) continue ;;
    esac
    info "  removing stale unit $installed_unit..."
    sudo systemctl disable "$installed_unit" 2>/dev/null || true
    sudo systemctl stop "$installed_unit" 2>/dev/null || true
    sudo rm -f /etc/systemd/system/"$installed_unit"
  done < <(systemctl list-unit-files --type=service,timer --no-pager --no-legend 2>/dev/null)
  sudo systemctl daemon-reload
fi

info "Setting up Next.js server..."
cd "$FRONTEND_DIR"

if ! command -v pm2 &> /dev/null; then
    info "Installing PM2 for process management..."
    sudo npm install -g pm2
fi

if ! sudo systemctl is-enabled --quiet pm2-god.service 2>/dev/null; then
    info "Configuring PM2 to survive reboot..."
    if ! sudo env PATH=$PATH:/usr/bin pm2 startup systemd -u god --hp /home/god > /dev/null 2>&1; then
        warn "pm2 startup failed — PM2 will not survive reboot"
    fi
fi

unset NEXT_PUBLIC_API_URL
export NEXT_PUBLIC_APP_ENV=production
rm -rf ~/.pm2

info "Cleaning up previous Next.js build artifacts..."
rm -rf .next

info "Building Next.js application..."
npm run build

pm2 start npm --name pifitness-next -- run start -- --port 3000 --hostname 127.0.0.1
pm2 save --force

if ! pm2 show pifitness-next | grep -q "status.*online"; then
    error_exit "PM2 failed to start Next.js server. Check logs with: pm2 logs pifitness-next"
fi

cd "$PROJECT_DIR"

# Start PRD units from systemd directory (no hardcoded unit names)
if [[ -d "$SYSTEMD_DIR" ]]; then
  for unit_file in "$SYSTEMD_DIR"/pifitness-*.service "$SYSTEMD_DIR"/pifitness-*.timer; do
    [[ -f "$unit_file" ]] || continue
    install_name="$(basename "$unit_file")"
    [[ "$install_name" == *-test.* ]] && continue
    sudo systemctl start "$install_name" 2>/dev/null || true
  done
fi
TARGET_PORT=8000

info "Updating nginx configuration..."
NGINX_TEMPLATE="$PROJECT_DIR/deploy/nginx/nginx-react.conf"
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

info "Testing nginx configuration..."
sudo nginx -t || error_exit "Nginx configuration test failed."

info "Reloading nginx..."
sudo systemctl reload nginx || error_exit "Failed to reload nginx."

info "Verifying nginx configuration..."
sudo nginx -T 2>/dev/null | grep -A5 "server_name pifitness.duckdns.org" | grep "proxy_pass" | grep -q ":${TARGET_PORT};" || warn "Nginx may not be using the expected port ${TARGET_PORT}"

cd /home/god/PiFitness/backups 2>/dev/null && ls -1t | tail -n +3 | xargs -r rm -rf 2>/dev/null || true

info "Deployment of react-ui completed successfully!"