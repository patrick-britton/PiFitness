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
    echo "ERROR: Commit not available in source repository: $COMMIT" >&2
    exit 1
fi

if ! git -C "$SOURCE_REPO" merge-base --is-ancestor \
    "$COMMIT" "origin/$GIT_BRANCH"; then
    echo "ERROR: Commit is not part of origin/$GIT_BRANCH." >&2
    exit 1
fi

if [[ -e "$APP_DIR" || -L "$APP_DIR" ]]; then
    echo "ERROR: TEST application path already exists: $APP_DIR" >&2
    echo "Refusing to overwrite it."
    exit 1
fi

echo "Creating TEST checkout..."
echo "Commit: $COMMIT"

git -C "$SOURCE_REPO" worktree add --detach "$APP_DIR" "$COMMIT"

ACTUAL_COMMIT=$(git -C "$APP_DIR" rev-parse HEAD)

if [[ "$ACTUAL_COMMIT" != "$COMMIT" ]]; then
    echo "ERROR: TEST checkout commit verification failed." >&2
    exit 1
fi

echo
echo "TEST checkout created successfully."
echo "Path:   $APP_DIR"
echo "Commit: $ACTUAL_COMMIT"
echo
echo "No dependencies installed. No migrations applied."
echo "No services changed."