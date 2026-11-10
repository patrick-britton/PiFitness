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

case "$ENVIRONMENT" in
    test)
        BRANCH="feature-test"
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

SOURCE_REPO="/home/god/PiFitness"

if [[ ! "$COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "ERROR: A full 40-character commit SHA is required." >&2
    exit 64
fi

[[ -f "$ENV_FILE" ]] || {
    echo "ERROR: Missing environment file: $ENV_FILE" >&2
    exit 1
}

git -C "$SOURCE_REPO" cat-file -e "${COMMIT}^{commit}"

git -C "$SOURCE_REPO" merge-base --is-ancestor \
    "$COMMIT" "origin/$BRANCH" || {
    echo "ERROR: Commit is not part of origin/$BRANCH." >&2
    exit 1
}

if [[ -e "$APP_DIR" || -L "$APP_DIR" ]]; then
    git -C "$SOURCE_REPO" worktree list --porcelain |
        grep -Fqx "worktree $APP_DIR" || {
            echo "ERROR: Existing TEST path is not a registered worktree." >&2
            exit 1
        }
fi

STAGE_DIR="${APP_DIR}.staging.$$"

if [[ -e "$STAGE_DIR" || -L "$STAGE_DIR" ]]; then
    echo "ERROR: Staging path already exists: $STAGE_DIR" >&2
    exit 1
fi

cleanup() {
    if git -C "$SOURCE_REPO" worktree list --porcelain |
        grep -Fqx "worktree $STAGE_DIR"; then

        if [[ ! -e "$APP_DIR" && ! -L "$APP_DIR" ]]; then
            git -C "$SOURCE_REPO" worktree move \
                "$STAGE_DIR" "$APP_DIR" || \
                git -C "$SOURCE_REPO" worktree remove --force "$STAGE_DIR"
        else
            git -C "$SOURCE_REPO" worktree remove --force "$STAGE_DIR"
        fi
    fi
}
trap cleanup EXIT

echo "Preparing clean TEST checkout..."
git -C "$SOURCE_REPO" worktree add --detach "$STAGE_DIR" "$COMMIT"

STAGE_ENV_LINK="$STAGE_DIR/backend/.env"
ln -s "$ENV_FILE" "$STAGE_ENV_LINK"

ACTUAL_COMMIT=$(git -C "$STAGE_DIR" rev-parse HEAD)

[[ "$ACTUAL_COMMIT" == "$COMMIT" ]] || {
    echo "ERROR: Staged commit verification failed." >&2
    exit 1
}

if [[ -e "$APP_DIR" || -L "$APP_DIR" ]]; then
    echo "Replacing previous TEST checkout..."
    git -C "$SOURCE_REPO" worktree remove --force "$APP_DIR"
fi

git -C "$SOURCE_REPO" worktree move "$STAGE_DIR" "$APP_DIR"

trap - EXIT

echo
echo "TEST checkout successfully replaced."
echo "Path:   $APP_DIR"
echo "Commit: $(git -C "$APP_DIR" rev-parse HEAD)"
echo "Environment link:"
ls -l "$APP_DIR/backend/.env"

echo
echo "No dependencies installed."
echo "No migrations applied."
echo "No services changed."