#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    echo "Usage: bash deploy/scripts/bootstrap.sh --environment prd|test --commit SHA"
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
    prd)
        GIT_BRANCH="main"
        APP_DIR="/home/god/PiFitness"
        ENV_FILE="/home/god/PiFitness/backend/.env"
        ;;
    test)
        GIT_BRANCH="feature-test"
        APP_DIR="/home/god/PiFitness-test"
        ENV_FILE="/home/god/.config/pifitness/test.env"
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

echo "PiFitness bootstrap"
echo "Environment:     $ENVIRONMENT"
echo "Git branch:      $GIT_BRANCH"
echo "Commit:          $COMMIT"
echo "Application:     $APP_DIR"
echo "Environment file: $ENV_FILE"

echo
echo "Bootstrap implementation is not yet complete."
echo "No deployment actions were performed."
exit 2