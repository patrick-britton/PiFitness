#!/usr/bin/env bash
# dbmate_guard.sh - Thin guard wrapper around dbmate for PiFitness.
#
# Re-implements migrate.py's safety gates on top of dbmate (dbmate itself has
# no PRD guard, localhost-only check, or APP_ENV marker validation).
#
# Usage:
#   dbmate_guard.sh status --env-file PATH [--allow-prd]
#   dbmate_guard.sh up   --env-file PATH [--allow-prd]
#   dbmate_guard.sh down --env-file PATH [--allow-prd]
#
# The wrapper loads connection settings exclusively from --env-file, validates
# the target (localhost-only, --allow-prd for prod, marker matches APP_ENV),
# then execs dbmate with DATABASE_URL exported so dbmate performs the actual
# migration work against deploy/migrations/.

set -euo pipefail

DBMATE_BIN="${DBMATE_BIN:-/usr/local/bin/dbmate}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-deploy/migrations}"

usage() {
    echo "Usage: $0 {status|up|down} --env-file PATH [--allow-prd]" >&2
    exit 2
}

# --- parse arguments ---
command="${1:-}"
shift || true
allow_prd=0
env_file=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --env-file)
            env_file="$2"
            shift 2
            ;;
        --allow-prd)
            allow_prd=1
            shift
            ;;
        *)
            usage
            ;;
    esac
done

[[ -n "$command" && -n "$env_file" ]] || usage
[[ "$command" =~ ^(status|up|down)$ ]] || usage

# --- load env file (dotenv format, no shell interpolation) ---
# Required: APP_ENV, PG_HOST, PG_DB, PG_USER, PG_PASSWORD
# Optional: PG_PORT (default 5432), PGSSLMODE (default disable)
if [[ ! -f "$env_file" ]]; then
    echo "ERROR: Migration environment file does not exist: $env_file" >&2
    exit 1
fi

APP_ENV=""
PG_HOST=""
PG_DB=""
PG_USER=""
PG_PASSWORD=""
PG_PORT="5432"
PGSSLMODE="disable"

while IFS='=' read -r key value || [[ -n "$key" ]]; do
    # Strip inline comments and surrounding whitespace/quotes.
    key="${key%%\#*}"
    key="${key## }"
    key="${key%% }"
    [[ -z "$key" ]] && continue
    case "$key" in
        APP_ENV)       APP_ENV="${value//\"/}" ;;
        PG_HOST)       PG_HOST="${value//\"/}" ;;
        PG_DB)         PG_DB="${value//\"/}" ;;
        PG_USER)       PG_USER="${value//\"/}" ;;
        PG_PASSWORD)   PG_PASSWORD="${value//\"/}" ;;
        PG_PORT)       PG_PORT="${value//\"/}" ;;
        PGSSLMODE)     PGSSLMODE="${value//\"/}" ;;
    esac
done < "$env_file"

APP_ENV="$(printf '%s' "$APP_ENV" | tr '[:upper:]' '[:lower:]')"
PG_HOST="$(printf '%s' "$PG_HOST" | tr '[:upper:]' '[:lower:]')"

# --- validate required settings ---
for var in APP_ENV PG_HOST PG_DB PG_USER PG_PASSWORD; do
    val="$(eval printf '%s' "\$$var")"
    [[ -n "$val" ]] || { echo "ERROR: $var is required in $env_file" >&2; exit 1; }
done

case "$APP_ENV" in
    dev|test|prod) ;;
    *)
        echo "ERROR: Unsupported APP_ENV=$APP_ENV; expected dev, test, or prod." >&2
        exit 1
        ;;
esac

# --- validate target: localhost-only host ---
case "$PG_HOST" in
    localhost|127.0.0.1|::1) ;;
    *)
        echo "ERROR: Refusing to migrate remote host $PG_HOST; migrations run only against localhost." >&2
        exit 1
        ;;
esac

# --- validate --allow-prd rules ---
if [[ "$APP_ENV" == "prod" && "$allow_prd" -eq 0 ]]; then
    echo "ERROR: PRD migrations require the explicit --allow-prd flag." >&2
    exit 1
fi
if [[ "$APP_ENV" != "prod" && "$allow_prd" -eq 1 ]]; then
    echo "ERROR: --allow-prd is valid only with APP_ENV=prod." >&2
    exit 1
fi

# --- build DATABASE_URL for dbmate ---
DATABASE_URL="postgres://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}?sslmode=${PGSSLMODE}"
export DATABASE_URL

# --- validate connection identity via psql ---
# Verify database name, loopback server address, and port match before DDL.
IDENTITY="$(psql "$DATABASE_URL" -tAc "
    SELECT current_database(), inet_server_addr()::text, inet_server_port()
" 2>/dev/null)" || {
    echo "ERROR: Unable to connect to ${PG_HOST}:${PG_PORT}/${PG_DB}." >&2
    exit 1
}

read -r actual_db server_addr actual_port <<< "$IDENTITY"

[[ "$actual_db" == "$PG_DB" ]] || {
    echo "ERROR: Connected to database $actual_db, expected $PG_DB." >&2
    exit 1
}
[[ -n "$server_addr" ]] || {
    echo "ERROR: PostgreSQL did not report a server address." >&2
    exit 1
}

# Normalize server address (strip CIDR prefix like ::1/128 or 127.0.0.1/32).
server_addr="${server_addr%%/*}"
case "$server_addr" in
    127.0.0.1|::1|localhost) ;;
    *)
        echo "ERROR: Server address $server_addr is not loopback; refusing." >&2
        exit 1
        ;;
esac
[[ "$actual_port" == "$PG_PORT" ]] || {
    echo "ERROR: Connected on port $actual_port, expected $PG_PORT." >&2
    exit 1
}

# --- validate database marker against APP_ENV ---
MARKER="$(psql "$DATABASE_URL" -tAc "SELECT current_setting('pifitness.env', true)" 2>/dev/null || printf '')"

case "$APP_ENV" in
    dev)
        [[ "$MARKER" == "dev" ]] || {
            echo "ERROR: APP_ENV=dev but database marker is ${MARKER:-<unset>}; expected dev." >&2
            exit 1
        }
        ;;
    test)
        [[ "$MARKER" == "test" ]] || {
            echo "ERROR: APP_ENV=test but database marker is ${MARKER:-<unset>}; expected test." >&2
            exit 1
        }
        ;;
    prod)
        [[ -z "$MARKER" ]] || {
            echo "ERROR: APP_ENV=prod but database marker is $MARKER; PRD must remain unmarked." >&2
            exit 1
        }
        ;;
esac

# --- delegate to dbmate ---
exec "$DBMATE_BIN" --migrations-dir "$MIGRATIONS_DIR" "$command"