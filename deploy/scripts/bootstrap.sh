#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    echo "Usage: bootstrap.sh --environment prd|test --commit SHA"
}

ENVIRONMENT=""
COMMIT=""

while (($#)); do
    case "$1" in
        --environment)
            [[ $# -ge 2 ]] || { usage; exit 64; }
            ENVIRONMENT="${2,,}"
            shift 2
            ;;
        --commit)
            [[ $# -ge 2 ]] || { usage; exit 64; }
            COMMIT="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 64
            ;;
    esac
done

SOURCE_REPO="/home/god/PiFitness"

case "$ENVIRONMENT" in
    test)
        GIT_BRANCH="feature-test"
        APP_DIR="/home/god/PiFitness-test"
        ENV_FILE="/home/god/.config/pifitness/test.env"
        ;;
    prd)
        echo "PRD deployment is not yet implemented."
        exit 2
        ;;
    *)
        echo "ERROR: Environment must be prd or test." >&2
        usage
        exit 64
        ;;
esac

if [[ ! "$COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "ERROR: A full 40-character Git commit SHA is required." >&2
    exit 64
fi

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: Environment file missing: $ENV_FILE" >&2
    exit 1
fi

if ! git -C "$SOURCE_REPO" cat-file -e "${COMMIT}^{commit}" 2>/dev/null; then
    echo "ERROR: Commit not available: $COMMIT" >&2
    exit 1
fi

if ! git -C "$SOURCE_REPO" merge-base --is-ancestor \
    "$COMMIT" "origin/$GIT_BRANCH"; then
    echo "ERROR: Commit is not part of origin/$GIT_BRANCH." >&2
    exit 1
fi

ENV_LINK="$APP_DIR/backend/.env"

ensure_env_link() {
    if [[ -L "$ENV_LINK" ]]; then
        ACTUAL=$(readlink -f "$ENV_LINK")
        EXPECTED=$(readlink -f "$ENV_FILE")
        if [[ "$ACTUAL" != "$EXPECTED" ]]; then
            echo "ERROR: Existing environment link points elsewhere: $ENV_LINK" >&2
            exit 1
        fi
    elif [[ -e "$ENV_LINK" ]]; then
        echo "ERROR: Unexpected file exists at $ENV_LINK" >&2
        exit 1
    else
        ln -s "$ENV_FILE" "$ENV_LINK"
    fi

    [[ -f "$ENV_LINK" ]] || {
        echo "ERROR: Environment link is not usable: $ENV_LINK" >&2
        exit 1
    }
}

if [[ -e "$APP_DIR" || -L "$APP_DIR" ]]; then
    if ! git -C "$SOURCE_REPO" worktree list --porcelain |
        grep -Fqx "worktree $APP_DIR"; then
        echo "ERROR: Existing TEST path is not a registered worktree." >&2
        exit 1
    fi

    ensure_env_link

    echo "Existing TEST checkout verified."
    echo "Current checkout: $(git -C "$APP_DIR" rev-parse HEAD)"
    echo "Environment link verified: $ENV_LINK"
    echo "Checkout update is not yet implemented."
    exit 0
fi

echo "Creating TEST checkout at $COMMIT..."
git -C "$SOURCE_REPO" worktree add --detach "$APP_DIR" "$COMMIT"

ensure_env_link

ACTUAL_COMMIT=$(git -C "$APP_DIR" rev-parse HEAD)

if [[ "$ACTUAL_COMMIT" != "$COMMIT" ]]; then
    echo "ERROR: TEST checkout commit verification failed." >&2
    exit 1
fi

echo
echo "TEST checkout created successfully."
echo "Path: $APP_DIR"
echo "Commit: $ACTUAL_COMMIT"
echo "Environment link: $ENV_LINK"